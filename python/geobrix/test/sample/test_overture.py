import os
import subprocess
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from test.sample._fake_overture_catalog import open_fake_overture  # noqa: E402

from pyspark.sql import Row  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import databricks.labs.gbx.sample.overture as ov_mod  # noqa: E402
import databricks.labs.gbx.sample.overture as ov  # noqa: E402
from databricks.labs.gbx.sample.overture import (  # noqa: E402
    _META_COLS,
    OvertureClient,
    _meta_dataframe,
)


def _fake_item_loader(href):
    """Offline item loader for tests: returns fake items keyed by href content."""
    if "sf-building" in href:

        class _A:
            href = "s3://overturemaps-us-west-2/release/buildings/building/sf.parquet"

        class _Item:
            bbox = [-122.52, 37.70, -122.36, 37.83]
            assets = {"data": _A()}

        return _Item()
    if "eu-place" in href:

        class _A:
            href = "s3://overturemaps-us-west-2/release/places/place/eu.parquet"

        class _Item:
            bbox = [10.0, 50.0, 11.0, 51.0]
            assets = {"data": _A()}

        return _Item()
    raise FileNotFoundError(href)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("overture-sp1-test")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_discover_columns_and_filter(spark):
    client = OvertureClient(
        release="2024-07-01",
        _catalog_opener=open_fake_overture,
        _item_loader=_fake_item_loader,
    )
    df = client.discover((-122.45, 37.74, -122.40, 37.78), themes=["buildings"])
    assert df.columns == ["theme", "type", "href", "asset_bbox", "release"]
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["theme"] == "buildings"
    assert rows[0]["release"] == "2024-07-01"
    assert rows[0]["asset_bbox"] == [-122.52, 37.70, -122.36, 37.83]


def test_discover_all_themes_when_none(spark):
    client = OvertureClient(
        release="2024-07-01",
        _catalog_opener=open_fake_overture,
        _item_loader=_fake_item_loader,
    )
    df = client.discover((-180, -90, 180, 90), themes=None)
    # fake catalog has a building + a place; both fall inside the world bbox
    assert {r["type"] for r in df.collect()} == {"building", "place"}


def _write_fake_overture_parquet(spark, path, bbox_struct=True):
    """Write a tiny GeoParquet-ish parquet with a bbox struct so pushdown can fire."""
    if bbox_struct:
        rows = [
            Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76)),
            Row(
                id=2, bbox=Row(xmin=10.0, ymin=50.0, xmax=10.1, ymax=50.1)
            ),  # outside SF
        ]
    else:
        rows = [Row(id=1), Row(id=2)]
    spark.createDataFrame(rows).write.mode("overwrite").parquet(path)


def test_download_distributed_writes_aoi_subset(spark, tmp_path):
    src = str(tmp_path / "src.parquet")
    _write_fake_overture_parquet(spark, src)
    out_dir = str(tmp_path / "out")

    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    assets = spark.createDataFrame(
        [
            (
                "buildings",
                "building",
                src,
                [-122.52, 37.70, -122.36, 37.83],
                "2024-07-01",
            )
        ],
        schema,
    )
    meta = client._download_distributed(
        assets,
        out_dir,
        bbox=(-122.45, 37.74, -122.40, 37.78),
        validate=True,
        partitions=4,
    )
    # Serverless-safety: hash-by-column repartition is NOT AQE-coalesced to 1.
    assert meta.rdd.getNumPartitions() > 1
    mrows = meta.collect()
    assert len(mrows) == 1
    written = mrows[0]["source"]
    assert written == mrows[0]["path"]  # source aliased as path
    assert os.path.isdir(written) or os.path.exists(written)
    # only the in-AOI row survived the bbox-struct pushdown
    subset = spark.read.parquet(written)
    assert subset.count() == 1
    assert subset.collect()[0]["id"] == 1


def test_download_distributed_multi_asset_no_clobber(spark, tmp_path):
    """Two assets sharing (theme, type) must both survive — no shard clobber.

    Before the fix: the 2nd asset's mode("overwrite") wiped the 1st asset's
    output dir; only the last shard survived; metadata source values collapsed
    to one (MERGE key (theme, type, source) collapsed to 1 row). After the fix:
    each asset writes to its own per-asset token subdir, both rows survive, and
    metadata has TWO distinct source values.

    Regression test for the review finding that two assets sharing (theme, type) clobbered each other.
    """
    # Asset A: shard 1 — ids 10, 11 (both inside AOI)
    src_a = str(tmp_path / "shard_a.parquet")
    spark.createDataFrame(
        [
            Row(id=10, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76)),
            Row(id=11, bbox=Row(xmin=-122.43, ymin=37.75, xmax=-122.42, ymax=37.76)),
        ]
    ).write.mode("overwrite").parquet(src_a)

    # Asset B: shard 2 — id 20 inside AOI, id 21 outside
    src_b = str(tmp_path / "shard_b.parquet")
    spark.createDataFrame(
        [
            Row(id=20, bbox=Row(xmin=-122.44, ymin=37.75, xmax=-122.43, ymax=37.76)),
            Row(id=21, bbox=Row(xmin=10.0, ymin=50.0, xmax=10.1, ymax=50.1)),
        ]
    ).write.mode("overwrite").parquet(src_b)

    out_dir = str(tmp_path / "out_multi")
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    # Both assets share theme="buildings", type="building"
    assets = spark.createDataFrame(
        [
            (
                "buildings",
                "building",
                src_a,
                [-122.52, 37.70, -122.36, 37.83],
                "2024-07-01",
            ),
            (
                "buildings",
                "building",
                src_b,
                [-122.52, 37.70, -122.36, 37.83],
                "2024-07-01",
            ),
        ],
        schema,
    )

    bbox = (-122.45, 37.74, -122.40, 37.78)
    meta = client._download_distributed(
        assets, out_dir, bbox=bbox, validate=True, partitions=4
    )
    mrows = meta.collect()

    # Metadata: TWO rows, each with a DISTINCT source (not collapsed)
    assert len(mrows) == 2, f"Expected 2 metadata rows, got {len(mrows)}"
    sources = {r["source"] for r in mrows}
    assert len(sources) == 2, f"Expected 2 distinct sources, got {sources}"
    # Both source dirs must exist
    for src in sources:
        assert os.path.isdir(src), f"Source dir missing: {src}"

    # Data: recursive read over theme/type picks up BOTH shards' AOI rows
    combined = client.read(out_dir, theme="buildings", type="building")
    ids = {r["id"] for r in combined.collect()}
    # Shard A contributes ids 10, 11; shard B contributes id 20 (21 is outside AOI)
    assert ids == {10, 11, 20}, f"Expected {{10, 11, 20}}, got {ids}"
    assert combined.count() == 3, f"Expected 3 rows total, got {combined.count()}"

    # Idempotency: re-running download keeps counts stable (no duplication, same sources)
    meta2 = client._download_distributed(
        assets, out_dir, bbox=bbox, validate=True, partitions=4
    )
    mrows2 = meta2.collect()
    assert len(mrows2) == 2, f"Idempotency: expected 2 metadata rows, got {len(mrows2)}"
    sources2 = {r["source"] for r in mrows2}
    assert sources2 == sources, f"Idempotency: sources changed: {sources2} != {sources}"
    combined2 = client.read(out_dir, theme="buildings", type="building")
    assert combined2.count() == 3, f"Idempotency: count changed to {combined2.count()}"


def test_empty_meta_dataframe_matches_meta_cols(spark):
    """Empty-result meta DataFrame must have the exact _META_COLS schema.

    Callers union meta DataFrames; a schema mismatch on the empty branch
    (missing path/last_update) causes a union error at runtime.
    """
    empty_meta = _meta_dataframe(spark, [], partitions=2)

    # Column names must match _META_COLS exactly (order included).
    assert empty_meta.columns == _META_COLS

    # path and last_update must carry the right types (not missing or string-only).
    schema_map = {f.name: f.dataType for f in empty_meta.schema}
    assert "path" in schema_map, "path column missing from empty meta DataFrame"
    assert isinstance(
        schema_map["path"], StringType
    ), f"path should be StringType, got {schema_map['path']}"
    assert (
        "last_update" in schema_map
    ), "last_update column missing from empty meta DataFrame"
    assert isinstance(
        schema_map["last_update"], TimestampType
    ), f"last_update should be TimestampType, got {schema_map['last_update']}"

    # DataFrame must be truly empty.
    assert empty_meta.count() == 0


def test_download_fallback_injected_fetcher(spark, tmp_path):
    # build a real source parquet, then serve its bytes through a fake _get_fn
    src = str(tmp_path / "asset.parquet")

    spark.createDataFrame([Row(id=1), Row(id=2)]).coalesce(1).write.mode(
        "overwrite"
    ).parquet(src)
    # the "asset" is a single part file inside src
    part = [f for f in os.listdir(src) if f.endswith(".parquet")][0]
    src_file = os.path.join(src, part)

    def fake_get(href, timeout=None, stream=False):
        class _Resp:
            status_code = 200

            def raise_for_status(self):
                pass

            def iter_content(self, n):
                with open(src_file, "rb") as fh:
                    while True:
                        chunk = fh.read(n)
                        if not chunk:
                            break
                        yield chunk

        return _Resp()

    client = OvertureClient(
        release="2024-07-01", _catalog_opener=open_fake_overture, _get_fn=fake_get
    )
    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    out_dir = str(tmp_path / "out_fb")
    assets = spark.createDataFrame(
        [
            (
                "places",
                "place",
                "http://fake/place.parquet",
                [0.0, 0.0, 1.0, 1.0],
                "2024-07-01",
            )
        ],
        schema,
    )
    meta = client._download_fallback(
        assets, out_dir, validate=True, max_tries=2, partitions=4
    )
    assert meta.rdd.getNumPartitions() > 1
    row = meta.collect()[0]
    assert row["is_out_file_valid"] is True
    assert os.path.exists(row["source"])
    assert row["source"] == row["path"]
    assert spark.read.parquet(row["source"]).count() == 2


def test_download_routes_cloud_to_distributed(spark, tmp_path):
    src = str(tmp_path / "cloudish.parquet")

    # The href is an absolute path (starts with "/"), so download()'s cloud
    # predicate treats it as a FUSE-mounted Volume path and routes to
    # _download_distributed (not the HTTP fallback).
    spark.createDataFrame(
        [Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76))]
    ).write.mode("overwrite").parquet(src)
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    assets = spark.createDataFrame(
        [
            (
                "buildings",
                "building",
                src,
                [-122.52, 37.70, -122.36, 37.83],
                "2024-07-01",
            )
        ],
        schema,
    )
    out_dir = str(tmp_path / "dl")
    meta = client.download(
        assets, out_dir, bbox=(-122.45, 37.74, -122.40, 37.78), partitions=4
    )
    assert meta.columns == _META_COLS
    first = meta.collect()
    assert len(first) == 1 and first[0]["is_out_file_valid"] is True
    # idempotent re-run: same target, still valid, no error
    meta2 = client.download(
        assets, out_dir, bbox=(-122.45, 37.74, -122.40, 37.78), partitions=4
    )
    assert meta2.collect()[0]["is_out_file_valid"] is True


def test_download_table_merge_idempotent(spark, tmp_path):
    pytest.importorskip("delta")
    # a SparkSession with Delta configured is required; skip if not available
    try:
        spark.sql("SELECT 1")  # sanity
        spark.range(1).write.format("delta").mode("overwrite").save(
            str(tmp_path / "_delta_probe")
        )
    except Exception:
        pytest.skip("Delta not enabled on this local SparkSession")

    src = str(tmp_path / "m.parquet")

    spark.createDataFrame(
        [Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76))]
    ).write.mode("overwrite").parquet(src)
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    assets = spark.createDataFrame(
        [
            (
                "buildings",
                "building",
                src,
                [-122.52, 37.70, -122.36, 37.83],
                "2024-07-01",
            )
        ],
        schema,
    )
    table = "overture_meta_test"
    out_dir = str(tmp_path / "dl2")
    client.download(
        assets,
        out_dir,
        bbox=(-122.45, 37.74, -122.40, 37.78),
        table=table,
        partitions=2,
    )
    client.download(
        assets,
        out_dir,
        bbox=(-122.45, 37.74, -122.40, 37.78),
        table=table,
        partitions=2,
    )
    # MERGE keyed by (theme, type, source) -> still exactly one row, not two
    assert spark.table(table).count() == 1
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_read_from_volume_dir(spark, tmp_path):
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    out_dir = str(tmp_path / "rd")
    target = os.path.join(out_dir, "buildings", "building")

    spark.createDataFrame(
        [Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76))]
    ).write.mode("overwrite").parquet(target)
    df = client.read(out_dir)
    assert df.count() == 1
    # bbox filter retains the in-AOI row
    df2 = client.read(out_dir, bbox=(-122.45, 37.74, -122.40, 37.78))
    assert df2.count() == 1
    df3 = client.read(out_dir, bbox=(0, 0, 1, 1))  # disjoint
    assert df3.count() == 0


def test_read_from_metadata_dataframe(spark, tmp_path):
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    target = str(tmp_path / "assetdir")

    spark.createDataFrame([Row(id=7)]).write.mode("overwrite").parquet(target)
    meta = spark.createDataFrame([(target,)], ["source"]).withColumn(
        "path", F.col("source")
    )
    df = client.read(meta)
    assert df.collect()[0]["id"] == 7


def test_download_routes_http_to_fallback(spark, tmp_path):
    """download() with an http:// href routes to _download_fallback (not distributed).

    The cloud predicate requires all hrefs to start with a cloud scheme or "/";
    an http:// URL matches neither, so the fallback path is taken. This test
    proves the routing — _download_fallback is not called directly.
    """
    # Build a real parquet file to serve via the injected fake _get_fn.
    src = str(tmp_path / "http_asset.parquet")
    spark.createDataFrame([Row(id=1), Row(id=2)]).coalesce(1).write.mode(
        "overwrite"
    ).parquet(src)
    part = [f for f in os.listdir(src) if f.endswith(".parquet")][0]
    src_file = os.path.join(src, part)

    def fake_get(href, timeout=None, stream=False):
        class _Resp:
            status_code = 200

            def raise_for_status(self):
                pass

            def iter_content(self, n):
                with open(src_file, "rb") as fh:
                    while True:
                        chunk = fh.read(n)
                        if not chunk:
                            break
                        yield chunk

        return _Resp()

    client = OvertureClient(
        release="2024-07-01", _catalog_opener=open_fake_overture, _get_fn=fake_get
    )
    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    # http:// href — fails the cloud predicate → _download_fallback is called
    assets = spark.createDataFrame(
        [
            (
                "places",
                "place",
                "http://fake-overture.example.com/place.parquet",
                [0.0, 0.0, 1.0, 1.0],
                "2024-07-01",
            )
        ],
        schema,
    )
    out_dir = str(tmp_path / "out_http")
    meta = client.download(assets, out_dir, partitions=4)
    # Result must carry the full _META_COLS schema in order.
    assert meta.columns == _META_COLS
    row = meta.collect()[0]
    assert row["is_out_file_valid"] is True
    assert os.path.exists(row["source"])
    assert row["source"] == row["path"]
    assert spark.read.parquet(row["source"]).count() == 2


def test_read_empty_metadata_raises(spark, tmp_path):
    """read() with an empty metadata DataFrame raises a clear ValueError.

    _read_paths([]) would IndexError without the guard; a ValueError with an
    actionable message is required (offline, no Delta).
    """
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    empty_meta = spark.createDataFrame(
        [], StructType([StructField("source", StringType())])
    )
    with pytest.raises(ValueError, match="no asset paths found"):
        client.read(empty_meta)


def test_read_from_table_name(spark, tmp_path):
    """read() with a Delta table name reads the underlying asset parquet rows.

    Gated on delta-spark being importable (skips cleanly offline).
    """
    pytest.importorskip("delta")
    # Verify Delta write actually works on this local SparkSession; skip if not.
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(
            str(tmp_path / "_delta_probe")
        )
    except Exception:
        pytest.skip("Delta not enabled on this local SparkSession")

    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)

    # Write a tiny parquet asset that client.read() will load.
    asset_dir = str(tmp_path / "tbl_asset")
    spark.createDataFrame([Row(id=42)]).write.mode("overwrite").parquet(asset_dir)

    # Build a minimal metadata DataFrame and persist it as a Delta table.
    table_name = "overture_read_test_tbl"
    meta_df = spark.createDataFrame([(asset_dir, asset_dir)], ["source", "path"])
    meta_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    try:
        df = client.read(table_name)
        rows = df.collect()
        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
        assert rows[0]["id"] == 42
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_download_via_cli_builds_meta(spark, tmp_path, monkeypatch):
    """_download_via_cli writes per-(theme,type) parquet and returns correct meta rows."""
    # Build a real parquet to serve as the CLI output.
    src = str(tmp_path / "cli_src.parquet")
    spark.createDataFrame([Row(id=1), Row(id=2)]).coalesce(1).write.mode(
        "overwrite"
    ).parquet(src)
    part = [f for f in os.listdir(src) if f.endswith(".parquet")][0]
    src_file = os.path.join(src, part)

    # Monkeypatch CLI availability.
    monkeypatch.setattr(
        ov_mod, "_overturemaps_cli_path", lambda: Path("/fake/overturemaps")
    )
    monkeypatch.setattr(ov_mod, "_overturemaps_cli_available", lambda: True)

    # Monkeypatch subprocess.run to copy our test parquet to the -o path.
    def fake_subprocess_run(cmd, **kwargs):
        o_idx = cmd.index("-o")
        out_path = cmd[o_idx + 1]
        import shutil as _shutil

        _shutil.copyfile(src_file, out_path)

        class _Done:
            returncode = 0
            stdout = ""
            stderr = ""

        return _Done()

    monkeypatch.setattr(subprocess, "run", fake_subprocess_run)

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    assets = spark.createDataFrame(
        [
            (
                "buildings",
                "building",
                "",
                [-122.52, 37.70, -122.36, 37.83],
                "2026-06-17.0",
            )
        ],
        schema,
    )
    client = OvertureClient(
        release="2026-06-17.0",
        _catalog_opener=open_fake_overture,
        _item_loader=_fake_item_loader,
    )
    out_dir = str(tmp_path / "cli_out_dir")

    meta = client._download_via_cli(
        assets, out_dir, bbox=(-122.45, 37.74, -122.40, 37.78), validate=True
    )
    rows = meta.collect()
    assert len(rows) == 1
    row = rows[0]
    assert row["theme"] == "buildings"
    assert row["type"] == "building"
    assert os.path.exists(row["source"])
    assert row["out_file_sz"] > 0
    assert row["is_out_file_valid"] is True
    assert row["href"] == ""


def _fake_run_factory(outcomes, out_writer=None):
    """Build a fake subprocess.run that yields the given (returncode, stderr)
    outcomes in order. Optionally writes the -o output file on a 0-returncode call.
    Records the flag (--stac / --no-stac) each call was made with.
    """
    calls = []

    def _fake_run(cmd, **kwargs):
        flag = (
            "--stac"
            if "--stac" in cmd
            else ("--no-stac" if "--no-stac" in cmd else "?")
        )
        calls.append(flag)
        rc, stderr = outcomes.pop(0)
        if rc == 0 and out_writer is not None:
            o_idx = cmd.index("-o")
            out_writer(cmd[o_idx + 1])

        class _R:
            pass

        r = _R()
        r.returncode = rc
        r.stdout = ""
        r.stderr = stderr
        return r

    _fake_run.calls = calls
    return _fake_run


def test_run_overture_download_succeeds_first_stac_no_fallback(tmp_path, monkeypatch):
    """First --stac attempt succeeds -> returns immediately, never tries --no-stac."""
    out = str(tmp_path / "b.parquet")
    fake = _fake_run_factory(
        outcomes=[(0, "")],
        out_writer=lambda p: open(p, "wb").write(b"x"),
    )
    monkeypatch.setattr(subprocess, "run", fake)
    ov_mod._run_overture_download(
        "/fake/overturemaps", bbox_str="0,0,1,1", type_="building", local_out=out
    )
    assert fake.calls == ["--stac"]  # no fallback needed
    assert os.path.exists(out)


def test_run_overture_download_retries_stac_then_falls_back_to_no_stac(
    tmp_path, monkeypatch
):
    """--stac fails its retries, then --no-stac fallback succeeds (returns cleanly)."""
    out = str(tmp_path / "b.parquet")
    fake = _fake_run_factory(
        outcomes=[
            (1, "catalog timeout"),  # --stac retry 1
            (1, "catalog timeout"),  # --stac retry 2 (retries=2 default)
            (0, ""),  # --no-stac fallback succeeds
        ],
        out_writer=lambda p: open(p, "wb").write(b"x"),
    )
    monkeypatch.setattr(subprocess, "run", fake)
    ov_mod._run_overture_download(
        "/fake/overturemaps", bbox_str="0,0,1,1", type_="building", local_out=out
    )
    assert fake.calls == ["--stac", "--stac", "--no-stac"]
    assert os.path.exists(out)


def test_run_overture_download_raises_with_stderr_when_all_fail(tmp_path, monkeypatch):
    """Every attempt fails -> RuntimeError that SURFACES the CLI stderr (not swallowed)."""
    out = str(tmp_path / "b.parquet")
    fake = _fake_run_factory(
        outcomes=[
            (1, "stac boom"),  # --stac retry 1
            (1, "stac boom"),  # --stac retry 2
            (1, "s3 access denied: the real cause"),  # --no-stac fallback
        ],
    )
    monkeypatch.setattr(subprocess, "run", fake)
    with pytest.raises(RuntimeError, match="s3 access denied: the real cause"):
        ov_mod._run_overture_download(
            "/fake/overturemaps", bbox_str="0,0,1,1", type_="building", local_out=out
        )
    assert fake.calls == ["--stac", "--stac", "--no-stac"]


def test_download_via_cli_idempotent_skips_when_valid(spark, tmp_path, monkeypatch):
    """_download_via_cli skips the subprocess when the target already exists and is valid.

    First call (target absent): CLI runner is invoked once.
    Second call (target present + valid): CLI runner is NOT invoked.
    Third call with force=True (target present + valid): CLI runner IS invoked again.
    """
    src = str(tmp_path / "cli_idem_src.parquet")
    spark.createDataFrame([Row(id=1)]).coalesce(1).write.mode("overwrite").parquet(src)
    part = [f for f in os.listdir(src) if f.endswith(".parquet")][0]
    src_file = os.path.join(src, part)

    monkeypatch.setattr(
        ov_mod, "_overturemaps_cli_path", lambda: Path("/fake/overturemaps")
    )
    monkeypatch.setattr(ov_mod, "_overturemaps_cli_available", lambda: True)

    call_count = {"n": 0}

    def fake_subprocess_run(cmd, **kwargs):
        call_count["n"] += 1
        o_idx = cmd.index("-o")
        out_path = cmd[o_idx + 1]
        import shutil as _shutil

        _shutil.copyfile(src_file, out_path)

        class _Done:
            returncode = 0
            stdout = ""
            stderr = ""

        return _Done()

    monkeypatch.setattr(subprocess, "run", fake_subprocess_run)

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    assets = spark.createDataFrame(
        [
            (
                "buildings",
                "building",
                "",
                [-122.52, 37.70, -122.36, 37.83],
                "2026-06-17.0",
            )
        ],
        schema,
    )
    client = OvertureClient(
        release="2026-06-17.0",
        _catalog_opener=open_fake_overture,
        _item_loader=_fake_item_loader,
    )
    out_dir = str(tmp_path / "cli_idem_out")
    bbox = (-122.45, 37.74, -122.40, 37.78)

    # First call — target absent; CLI must run.
    client._download_via_cli(assets, out_dir, bbox=bbox, validate=False)
    assert (
        call_count["n"] == 1
    ), f"Expected 1 CLI call on first download, got {call_count['n']}"

    # Second call — target present + valid; CLI must NOT run.
    client._download_via_cli(assets, out_dir, bbox=bbox, validate=False)
    assert (
        call_count["n"] == 1
    ), f"Expected still 1 CLI call after idempotent skip, got {call_count['n']}"

    # Third call with force=True — must re-invoke the CLI even though target is valid.
    client._download_via_cli(assets, out_dir, bbox=bbox, validate=False, force=True)
    assert (
        call_count["n"] == 2
    ), f"Expected 2 CLI calls after force=True, got {call_count['n']}"


def test_download_via_cli_force_via_download_api(spark, tmp_path, monkeypatch):
    """download(..., force=True) is threaded through to _download_via_cli."""
    src = str(tmp_path / "force_src.parquet")
    spark.createDataFrame([Row(id=1)]).coalesce(1).write.mode("overwrite").parquet(src)
    part = [f for f in os.listdir(src) if f.endswith(".parquet")][0]
    src_file = os.path.join(src, part)

    monkeypatch.setattr(
        ov_mod, "_overturemaps_cli_path", lambda: Path("/fake/overturemaps")
    )
    monkeypatch.setattr(ov_mod, "_overturemaps_cli_available", lambda: True)

    call_count = {"n": 0}

    def fake_subprocess_run(cmd, **kwargs):
        call_count["n"] += 1
        o_idx = cmd.index("-o")
        out_path = cmd[o_idx + 1]
        import shutil as _shutil

        _shutil.copyfile(src_file, out_path)

        class _Done:
            returncode = 0
            stdout = ""
            stderr = ""

        return _Done()

    monkeypatch.setattr(subprocess, "run", fake_subprocess_run)

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    assets = spark.createDataFrame(
        [
            (
                "buildings",
                "building",
                "",
                [-122.52, 37.70, -122.36, 37.83],
                "2026-06-17.0",
            )
        ],
        schema,
    )
    client = OvertureClient(
        release="2026-06-17.0",
        _catalog_opener=open_fake_overture,
        _item_loader=_fake_item_loader,
    )
    out_dir = str(tmp_path / "force_out")
    bbox = (-122.45, 37.74, -122.40, 37.78)

    # First call (no force): CLI runs.
    client.download(assets, out_dir, bbox=bbox, validate=False)
    assert call_count["n"] == 1

    # Second call (no force): idempotent skip.
    client.download(assets, out_dir, bbox=bbox, validate=False)
    assert call_count["n"] == 1

    # Third call (force=True): CLI runs again.
    client.download(assets, out_dir, bbox=bbox, validate=False, force=True)
    assert call_count["n"] == 2


def test_download_overture_aoi_one_shot(spark, tmp_path, monkeypatch):
    # Force the convenience fn's default client to use the fake opener (offline).
    src = str(tmp_path / "aoi.parquet")

    spark.createDataFrame(
        [Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76))]
    ).write.mode("overwrite").parquet(src)

    orig_init = ov.OvertureClient.__init__

    def patched_init(self, *a, **k):
        k["_catalog_opener"] = open_fake_overture
        k["_item_loader"] = _fake_item_loader
        orig_init(self, *a, **k)

    monkeypatch.setattr(ov.OvertureClient, "__init__", patched_init)

    # The fake catalog's SF building href points at s3://...; rewrite discover to our local src
    orig_discover = ov.OvertureClient.discover

    def patched_discover(self, bbox, themes=None, release=None):
        df = orig_discover(self, bbox, themes=themes, release=release)

        return df.withColumn("href", F.lit(src))

    monkeypatch.setattr(ov.OvertureClient, "discover", patched_discover)

    out_dir = str(tmp_path / "oneshot")
    meta = ov.download_overture_aoi(
        (-122.45, 37.74, -122.40, 37.78),
        out_dir,
        themes=["buildings"],
        release="2024-07-01",
    )
    rows = meta.collect()
    assert len(rows) == 1
    assert rows[0]["theme"] == "buildings"
    assert rows[0]["is_out_file_valid"] is True
