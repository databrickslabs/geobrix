"""Tests for StacClient covering both driver-shim and UDF fan-out paths.

UDF-path tests exercise the REAL repartition+explode+UDF code path (not the
_search_driver shim). The Python UDF workers run in separate subprocesses, so
monkeypatching the driver's pystac_client does not propagate. Instead we ship a
stub pystac_client.py to the workers via spark.sparkContext.addPyFile(), which
places it first on sys.path so the UDF imports it instead of the real package.
This exercises _ITEM_SCHEMA, _ASSET_SCHEMA, repartition, explode, and
dropDuplicates end-to-end.

NOTE: addPyFile with the same filename can only be called once per SparkContext
(subsequent calls with a different file content raise a conflict error). Therefore
we use a single stub file (added at module scope via a session-scoped fixture) that
always returns 2 items per search, with get_item() support for the download path.
Tests that check for "no duplicate (item_id, asset_name)" simply verify that
dropDuplicates works across multiple AOIs producing the same item.
"""

import os

import pytest
from pyspark.sql import SparkSession

from databricks.labs.gbx.stac import StacClient

# search_one retries a failing catalog with tenacity's exponential wait (4 s and up).
pytestmark = pytest.mark.usefixtures("no_backoff_sleep")

_ITEM = {
    "id": "S2_X",
    "collection": "sentinel-2-l2a",
    "bbox": [1.0, 2.0, 3.0, 4.0],
    "properties": {"datetime": "2022-06-01T19:49:11Z"},
    "assets": {
        "B02": {"href": "http://x/B02.tif"},
        "B03": {"href": "http://x/B03.tif"},
    },
}

# ---------------------------------------------------------------------------
# Single combined fake pystac_client stub (shipped to UDF workers ONCE)
#
# Returns 2 copies of the same item per search → used to test dedup as well
# as schema/columns. get_item() is provided for the download UDF path.
# ---------------------------------------------------------------------------

_PYSTAC_CLIENT_STUB = '''\
"""Fake pystac_client shipped to Spark UDF workers for the fan-out tests.
Returns 2 copies of S2_X (with B02+B03) per AOI search.
get_item() returns an item with B02 for the download UDF.
"""

_ITEM_DICT = {
    "id": "S2_X",
    "collection": "sentinel-2-l2a",
    "bbox": [1.0, 2.0, 3.0, 4.0],
    "properties": {"datetime": "2022-06-01T19:49:11Z", "eo:cloud_cover": 5},
    "assets": {
        "B02": {"href": "http://fake/B02.tif"},
        "B03": {"href": "http://fake/B03.tif"},
    },
}


class _FakeItem:
    def to_dict(self):
        return _ITEM_DICT


class _FakeSearch:
    def item_collection(self):
        return [_FakeItem(), _FakeItem()]


class _Asset:
    href = "http://fake/B02.tif"


class _PCItem:
    assets = {"B02": _Asset()}


class _FakeCatalog:
    def search(self, collections, intersects, datetime):
        return _FakeSearch()

    def get_item(self, item_id):
        return _PCItem()


class Client:
    @staticmethod
    def open(url, modifier=None):
        return _FakeCatalog()
'''


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[2]").appName("stac-test").getOrCreate()
    yield s
    s.stop()


@pytest.fixture(scope="module")
def spark_with_fake_catalog(spark, tmp_path_factory):
    """Module-scoped: ships the pystac_client stub once to the SparkContext.

    addPyFile with the same filename can only be called once per SparkContext;
    a module-scoped fixture ensures a single call.
    """
    stub_path = tmp_path_factory.mktemp("stubs") / "pystac_client.py"
    stub_path.write_text(_PYSTAC_CLIENT_STUB)
    spark.sparkContext.addPyFile(str(stub_path))
    return spark


# ---------------------------------------------------------------------------
# Existing driver-path tests (kept for regression coverage)
# ---------------------------------------------------------------------------


class _FakeItem:
    def __init__(self, d):
        self._d = d

    def to_dict(self):
        return self._d


class _FakeSearch:
    def item_collection(self):
        return [_FakeItem(_ITEM)]


class _FakeCatalog:
    def search(self, collections, intersects, datetime):
        return _FakeSearch()


def test_search_explodes_items_to_asset_rows(spark):
    client = StacClient(_catalog_opener=lambda: _FakeCatalog())
    df = spark.createDataFrame(
        [("cellA", '{"type":"Point","coordinates":[1,2]}')], ["cellid", "geojson"]
    )
    out = client.search(
        df,
        geojson_col="geojson",
        collections=["sentinel-2-l2a"],
        datetime="2022-06-01",
        partitions=2,
    )
    rows = {(r["cellid"], r["item_id"], r["asset_name"]) for r in out.collect()}
    assert rows == {("cellA", "S2_X", "B02"), ("cellA", "S2_X", "B03")}
    one = out.filter("asset_name = 'B02'").first()
    assert one["date"] == "2022-06-01" and one["href"] == "http://x/B02.tif"


# ---------------------------------------------------------------------------
# UDF fan-out path tests (stub pystac_client shipped to workers)
# ---------------------------------------------------------------------------


def test_search_udf_path_columns_and_types(spark_with_fake_catalog):
    """Real repartition+explode+UDF path produces correct columns/types.

    The stub returns 2 copies of S2_X; dropDuplicates collapses to {B02, B03}.
    """
    spark = spark_with_fake_catalog
    client = StacClient(catalog="http://fake-catalog", sign=None)
    df = spark.createDataFrame(
        [("cellA", '{"type":"Point","coordinates":[1,2]}')], ["cellid", "geojson"]
    )
    out = client.search(
        df,
        geojson_col="geojson",
        collections=["sentinel-2-l2a"],
        datetime="2022-06-01",
        partitions=2,
    )
    rows = out.collect()
    names = {r["asset_name"] for r in rows}
    assert names == {"B02", "B03"}, f"Expected B02 and B03 assets; got {names}"
    schema_names = {f.name for f in out.schema}
    assert {
        "item_id",
        "date",
        "item_bbox",
        "item_properties",
        "asset_name",
        "href",
        "cellid",
    } <= schema_names
    r0 = rows[0]
    assert r0["item_id"] == "S2_X"
    assert r0["date"] == "2022-06-01"
    assert isinstance(r0["item_bbox"], list) and len(r0["item_bbox"]) == 4


def test_search_udf_path_carries_non_geojson_cols(spark_with_fake_catalog):
    """Carried columns (non-geojson) are preserved through UDF path."""
    spark = spark_with_fake_catalog
    client = StacClient(catalog="http://fake-catalog", sign=None)
    df = spark.createDataFrame(
        [("region_A", 42, '{"type":"Point","coordinates":[1,2]}')],
        ["region", "score", "geojson"],
    )
    out = client.search(
        df,
        geojson_col="geojson",
        collections=["sentinel-2-l2a"],
        datetime="2022-06-01",
        partitions=2,
    )
    row = out.filter("asset_name = 'B02'").first()
    assert row["region"] == "region_A"
    assert row["score"] == 42


def test_search_udf_path_dedup_duplicate_items(spark_with_fake_catalog):
    """dropDuplicates(["item_id","asset_name"]) collapses duplicate rows.

    Two AOIs each returning 2 copies of S2_X × {B02, B03} via the stub.
    Raw output = up to 8 rows; after dropDuplicates = exactly {B02, B03}.
    """
    spark = spark_with_fake_catalog
    client = StacClient(catalog="http://fake-catalog", sign=None)
    df = spark.createDataFrame(
        [
            ('{"type":"Point","coordinates":[1,2]}',),
            ('{"type":"Point","coordinates":[3,4]}',),
        ],
        ["geojson"],
    )
    out = client.search(
        df,
        geojson_col="geojson",
        collections=["sentinel-2-l2a"],
        datetime="2022-06-01",
        partitions=2,
    )
    rows = out.collect()
    pairs = [(r["item_id"], r["asset_name"]) for r in rows]
    assert len(pairs) == len(
        set(pairs)
    ), f"Duplicate (item_id, asset_name) pairs after dropDuplicates: {pairs}"
    assert set(p[1] for p in pairs) == {"B02", "B03"}


def test_download_udf_path_publishes_valid_bytes(spark_with_fake_catalog, tmp_path):
    """Download UDF path writes bytes to Volume path and sets is_out_file_valid=True.

    The pystac_client stub (shipped once at module scope via addPyFile) handles
    get_item() in the _fetch UDF. We inject a fake HTTP fetcher via _get_fn= so
    no real network call is made. cloudpickle serialises the closure into the UDF
    worker, so the fake get= arrives in the worker subprocess intact.
    This exercises _ITEM_SCHEMA/_ASSET_SCHEMA, repartition, _fetch UDF, and
    fetch_validate_publish end-to-end.
    """
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    spark = spark_with_fake_catalog
    out_dir = tmp_path / "volume"

    # Build a real GTiff to serve as fake download bytes
    src = tmp_path / "fake_asset.tif"
    with rasterio.open(
        str(src),
        "w",
        driver="GTiff",
        height=8,
        width=8,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 8, 1, 1),
    ) as dst:
        dst.write((np.arange(64, dtype="uint8")).reshape(1, 8, 8))
    raw_bytes = src.read_bytes()

    def fake_get(href, timeout=None, stream=None):
        class _Resp:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield raw_bytes

        return _Resp()

    search_df = spark.createDataFrame(
        [("S2_X", "B02", "http://fake/B02.tif")],
        ["item_id", "asset_name", "href"],
    )
    client = StacClient(catalog="http://fake-catalog", sign=None)
    result = client.download(
        search_df,
        str(out_dir),
        asset_names=["B02"],
        name="{asset_name}_{item_id}.tif",
        validate=True,
        max_tries=2,
        partitions=1,
        _get_fn=fake_get,
    )
    row = result.first()
    assert row["is_out_file_valid"] is True
    assert row["out_file_path"] is not None
    assert "last_update" in {f.name for f in result.schema}


def test_download_fetches_only_the_hosts_allowed(spark, tmp_path):
    """StacClient(allowed_hosts=...) reaches the download UDF's fetch: an href off the list is refused before any
    request (the fetcher leaves a marker when it is called, in the UDF's process), one on it is fetched.
    """
    marker = tmp_path / "fetched"

    def fake_get(href, timeout=None, stream=None):
        marker.write_text(href)

        class _Resp:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield b"x" * 2048

        return _Resp()

    df = spark.createDataFrame(
        [("S2_X", "B02", "http://fake/B02.tif")], ["item_id", "asset_name", "href"]
    )

    def download(allowed):
        client = StacClient(
            catalog="http://fake-catalog", sign=None, allowed_hosts=allowed
        )
        return client.download(
            df,
            str(tmp_path / "out"),
            validate=False,
            max_tries=1,
            partitions=1,
            _get_fn=fake_get,
        ).first()

    assert download(["only.example"])["out_file_path"] is None
    assert not marker.exists()
    assert download(["fake"])["out_file_path"] is not None
    assert marker.exists()


# ---------------------------------------------------------------------------
# validate=False publishes without rasterio decode
# ---------------------------------------------------------------------------


def test_fetch_validate_false_publishes_non_decodable(tmp_path):
    """With validate=False, bytes that are not a valid raster are published."""
    from databricks.labs.gbx.stac._download import fetch_validate_publish

    garbage = b"not-a-raster-but-non-empty" * 10
    call_count = {"n": 0}

    def fake_get(href, timeout=None, stream=None):
        call_count["n"] += 1

        class _Resp:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield garbage

        return _Resp()

    out_dir = tmp_path / "out"
    res = fetch_validate_publish(
        lambda: "http://x/garbage.tif",
        str(out_dir),
        "garbage.tif",
        get=fake_get,
        max_tries=1,
        validate=False,
    )
    assert res is not None, "validate=False should publish non-decodable bytes"
    assert os.path.exists(res)
    assert (
        call_count["n"] == 1
    ), "Should not retry when validate=False and download succeeded"


def test_fetch_validate_true_rejects_garbage(tmp_path):
    """With validate=True, a truncated/garbage body is rejected (original behavior)."""
    from databricks.labs.gbx.stac._download import fetch_validate_publish

    def fake_get(href, timeout=None, stream=None):
        class _Resp:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield b"<Error>throttled</Error>"

        return _Resp()

    out_dir = tmp_path / "out"
    res = fetch_validate_publish(
        lambda: "http://x/bad.tif",
        str(out_dir),
        "bad.tif",
        get=fake_get,
        max_tries=2,
        sleep=lambda s: None,
        validate=True,
    )
    assert res is None, "validate=True must reject garbage body"


# ---------------------------------------------------------------------------
# repair includes last_update
# ---------------------------------------------------------------------------


def test_download_emits_last_update_column(spark_with_fake_catalog, tmp_path):
    """Download DataFrame includes a last_update timestamp column."""
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    spark = spark_with_fake_catalog

    src = tmp_path / "t.tif"
    with rasterio.open(
        str(src),
        "w",
        driver="GTiff",
        height=4,
        width=4,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 4, 1, 1),
    ) as dst:
        dst.write((np.arange(16, dtype="uint8")).reshape(1, 4, 4))
    raw_bytes = src.read_bytes()

    def fake_get(href, timeout=None, stream=None):
        class _R:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield raw_bytes

        return _R()

    out_dir = tmp_path / "out"
    search_df = spark.createDataFrame(
        [("S2_X", "B02", "http://fake/B02.tif")],
        ["item_id", "asset_name", "href"],
    )
    client = StacClient(catalog="http://fake-catalog", sign=None)
    result = client.download(
        search_df,
        str(out_dir),
        asset_names=["B02"],
        validate=True,
        max_tries=2,
        partitions=1,
        _get_fn=fake_get,
    )
    col_names = {f.name for f in result.schema}
    assert (
        "last_update" in col_names
    ), f"last_update column missing; schema: {col_names}"


# ---------------------------------------------------------------------------
# range-scan fan-out: N tasks, no Exchange in plan
# ---------------------------------------------------------------------------


def test_download_range_scan_fanout(spark, tmp_path):
    """Download uses spark.range fan-out so N=4 targets → 4 partitions.

    Asserts:
      (a) result.rdd.getNumPartitions() == 4  (range scan survives AQE)
      (b) all 4 targets produce one output row with a non-null out_file_path
      (c) the physical plan contains 'Range' (confirming the range-scan approach)

    Note: asserting absence of 'Exchange' is brittle because downstream
    withColumn calls can re-introduce one; getNumPartitions is the robust signal.
    """
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    from databricks.labs.gbx.stac import StacClient

    # Build 4 real GTiff files to serve as local "downloaded" assets.
    src_paths = []
    for i in range(4):
        src = tmp_path / f"asset_{i}.tif"
        with rasterio.open(
            str(src),
            "w",
            driver="GTiff",
            height=4,
            width=4,
            count=1,
            dtype="uint8",
            crs="EPSG:4326",
            transform=from_origin(0, 4, 1, 1),
        ) as dst:
            dst.write((np.arange(16, dtype="uint8")).reshape(1, 4, 4))
        src_paths.append(str(src))

    out_dir = tmp_path / "volume"

    # Build a 4-row DataFrame with local-file hrefs (no network, no signing needed).
    rows = [(f"item{i}", f"band{i}", src_paths[i]) for i in range(4)]
    search_df = spark.createDataFrame(rows, ["item_id", "asset_name", "href"])

    client = StacClient.__new__(StacClient)
    client.sign = None  # identity signer

    # bbox=(1,1,3,3) windows each local GTiff — windowed_download opens local
    # paths directly via rasterio without going through requests.get, so no
    # fake HTTP getter is needed.
    result = client.download(
        search_df,
        str(out_dir),
        validate=True,
        max_tries=1,
        partitions=4,
        bbox=(1, 1, 3, 3),
    )

    # (a) 4 partitions from the range scan
    assert (
        result.rdd.getNumPartitions() == 4
    ), f"Expected 4 partitions (range-scan fan-out); got {result.rdd.getNumPartitions()}"

    # (b) all 4 rows have a valid output path
    collected = result.collect()
    assert len(collected) == 4, f"Expected 4 result rows; got {len(collected)}"
    for row in collected:
        assert (
            row["out_file_path"] is not None
        ), f"out_file_path is None for {row['item_id']}/{row['asset_name']}"
        assert (
            row["is_out_file_valid"] is True
        ), f"is_out_file_valid is False for {row['item_id']}/{row['asset_name']}"

    # (c) physical plan contains 'Range' (confirms range-scan approach, not shuffle)
    plan_str = result._jdf.queryExecution().executedPlan().toString()
    assert (
        "Range" in plan_str
    ), f"Expected 'Range' in physical plan (range-scan fan-out); plan: {plan_str[:500]}"


# ---------------------------------------------------------------------------
# idempotency: already-valid file is not re-fetched
# ---------------------------------------------------------------------------


def test_fetch_skips_existing_valid_file(tmp_path):
    """If the target file already exists and is valid, no fetch is performed."""
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    from databricks.labs.gbx.stac._download import fetch_validate_publish

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    existing = out_dir / "ok.tif"
    with rasterio.open(
        str(existing),
        "w",
        driver="GTiff",
        height=4,
        width=4,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 4, 1, 1),
    ) as dst:
        dst.write((np.arange(16, dtype="uint8")).reshape(1, 4, 4))

    fetch_count = {"n": 0}

    def fake_get(href, timeout=None, stream=None):
        fetch_count["n"] += 1

        class _Resp:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield b"should-not-be-called"

        return _Resp()

    res = fetch_validate_publish(
        lambda: "http://x/ok.tif",
        str(out_dir),
        "ok.tif",
        get=fake_get,
        max_tries=3,
        validate=True,
    )
    assert res == str(existing)
    assert fetch_count["n"] == 0, "Should not fetch when valid file already exists"


def test_fetch_skips_existing_no_validate(tmp_path):
    """With validate=False, an existing file above the floor size is skipped."""
    from databricks.labs.gbx.stac._download import fetch_validate_publish

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    existing = out_dir / "ok.bin"
    existing.write_bytes(b"A" * 1024)  # non-empty, above floor

    fetch_count = {"n": 0}

    def fake_get(href, timeout=None, stream=None):
        fetch_count["n"] += 1

        class _Resp:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield b"should-not-be-called"

        return _Resp()

    res = fetch_validate_publish(
        lambda: "http://x/ok.bin",
        str(out_dir),
        "ok.bin",
        get=fake_get,
        max_tries=3,
        validate=False,
    )
    assert res == str(existing)
    assert fetch_count["n"] == 0, "Should not fetch when validate=False and file exists"


# ---------------------------------------------------------------------------
# path traversal sanitization
# ---------------------------------------------------------------------------


def test_filename_sanitized_against_path_traversal(tmp_path):
    """An item_id containing ../ does not escape out_dir."""
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    from databricks.labs.gbx.stac._download import fetch_validate_publish

    src = tmp_path / "src.tif"
    with rasterio.open(
        str(src),
        "w",
        driver="GTiff",
        height=4,
        width=4,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 4, 1, 1),
    ) as dst:
        dst.write((np.arange(16, dtype="uint8")).reshape(1, 4, 4))
    raw = src.read_bytes()

    def fake_get(href, timeout=None, stream=None):
        class _R:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield raw

        return _R()

    out_dir = tmp_path / "out"
    # filename contains path-traversal components
    dangerous_filename = "../../../etc/evil_B02_../evil/item.tif"
    res = fetch_validate_publish(
        lambda: "http://x/evil.tif",
        str(out_dir),
        dangerous_filename,
        get=fake_get,
        max_tries=1,
        validate=True,
    )
    assert res is not None
    # The resolved path must be under out_dir
    assert os.path.commonpath([str(out_dir), res]) == str(
        out_dir
    ), f"Path traversal escaped out_dir: res={res}, out_dir={out_dir}"


# ---------------------------------------------------------------------------
# warn on swallowed search exceptions
# ---------------------------------------------------------------------------


def test_search_one_warns_on_exception():
    """search_one emits a warning when the catalog raises."""
    import warnings

    from databricks.labs.gbx.stac._search import search_one

    class _FailCatalog:
        def search(self, collections, intersects, datetime):
            raise RuntimeError("network down")

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = search_one(
            _FailCatalog(),
            ["sentinel-2-l2a"],
            "2022-06-01",
            '{"type":"Point","coordinates":[1,2]}',
        )
    assert result == []
    assert any(
        "network down" in str(warning.message) for warning in w
    ), f"Expected warning about 'network down'; got: {[str(x.message) for x in w]}"


# ---------------------------------------------------------------------------
# Integration (real Planetary Computer, skipped in unit CI)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pc_search_and_download_one_asset(spark, tmp_path):
    client = StacClient()  # real Planetary Computer + sign_inplace
    df = spark.createDataFrame(
        [('{"type":"Point","coordinates":[-131.6,55.3]}',)], ["geojson"]
    )
    assets = client.search(
        df,
        geojson_col="geojson",
        collections=["sentinel-2-l2a"],
        datetime="2022-06-01/2022-06-05",
        partitions=1,
    )
    assets = assets.filter("asset_name = 'B02'").limit(1)
    assert assets.count() == 1
    files = client.download(assets, str(tmp_path), asset_names=["B02"], max_tries=3)
    row = files.first()
    assert row["is_out_file_valid"] is True
