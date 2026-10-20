"""on_change helpers: hashing, write planning, batch writes (Delta parts mocked)."""

import hashlib
import json
import os
import re
import types
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from databricks.labs.gbx.ds import _quarantine as Q
from databricks.labs.gbx.ds import changes as C

T0 = datetime(2026, 10, 17, tzinfo=timezone.utc)


def ev(path, h, minutes=0):
    return {
        "_source_path": path,
        "_source_hash": h,
        "_source_mtime": T0 + timedelta(minutes=minutes),
    }


def test_modes_constant():
    assert C.ON_CHANGE_MODES == ("scd2", "replace", "append")


def test_hash_files_sha256(spark, tmp_path):
    p = tmp_path / "a.bin"
    p.write_bytes(b"hello")
    df = spark.read.format("binaryFile").load(str(p))
    r = C.hash_files(df).collect()[0]
    assert r["_source_hash"] == hashlib.sha256(b"hello").hexdigest()
    assert r["_source_path"] == str(p) and r["_source_size"] == 5


@pytest.mark.parametrize("prefix", ["dbfs:", "file:"])
def test_hash_files_strips_scheme_prefix(spark, tmp_path, prefix):
    p = tmp_path / "a.bin"
    p.write_bytes(b"hello")
    df = spark.createDataFrame(
        [(prefix + str(p), T0, 5)],
        "path string, modificationTime timestamp, length long",
    )
    r = C.hash_files(df).collect()[0]
    assert r["_source_path"] == str(p)
    assert r["_source_hash"] == hashlib.sha256(b"hello").hexdigest()


def test_hash_batches_skips_vanished_file(tmp_path, monkeypatch):
    here = tmp_path / "here.bin"
    here.write_bytes(b"hello")
    gone = str(tmp_path / "gone.bin")
    monkeypatch.setattr(C._listing, "_retry_transient", lambda fn, **k: fn())
    batch = pa.record_batch(
        [
            pa.array([gone, str(here)]),
            pa.array([1, 2], pa.int64()),
        ],
        names=["path", "length"],
    )
    (out,) = list(C._hash_batches([batch]))
    assert out.column("path").to_pylist() == [str(here)]
    assert out.column("_source_hash").to_pylist() == [
        hashlib.sha256(b"hello").hexdigest()
    ]


def test_plan_writes_append_writes_everything():
    events = [ev("/a", "h1"), ev("/a", "h1", 1)]
    assert len(C.plan_writes(events, {"/a": {"h1"}}, "append")) == 2


def test_plan_writes_replace_skips_unchanged():
    assert C.plan_writes([ev("/a", "h1", 5)], {"/a": {"h1"}}, "replace") == []


def test_plan_writes_replace_newest_per_path_same_batch():
    out = C.plan_writes([ev("/a", "h1", 1), ev("/a", "h2", 2)], {}, "replace")
    assert [e["_source_hash"] for e in out] == ["h2"]


def test_plan_writes_scd2_revert_writes_nothing():
    assert C.plan_writes([ev("/a", "h1", 9)], {"/a": {"h1", "h2"}}, "scd2") == []


def test_plan_writes_scd2_writes_only_newest_event_per_path_in_batch():
    out = C.plan_writes(
        [ev("/a", "h2", 3), ev("/a", "h3", 4), ev("/a", "h2", 5), ev("/b", "h9", 1)],
        {"/a": {"h1"}},
        "scd2",
    )
    assert sorted(
        (e["_source_path"], e["_source_hash"], e["_source_mtime"].minute) for e in out
    ) == [
        ("/a", "h2", 5),
        ("/b", "h9", 1),
    ]


def test_plan_writes_rejects_unknown_mode():
    with pytest.raises(ValueError, match="scd2"):
        C.plan_writes([], {}, "upsert")


def test_replace_predicate_escapes_quotes_and_backslashes():
    pred = C.replace_predicate(
        ["/Volumes/c/s/v/o'brien file.gpkg", "/Volumes/c/s/v/é.gpkg", "/v/a\\b"]
    )
    assert pred == (
        "_source_path IN ('/Volumes/c/s/v/o\\'brien file.gpkg', '/Volumes/c/s/v/é.gpkg', '/v/a\\\\b')"
    )


def test_replace_predicate_selects_exactly_the_quoted_paths(spark):
    wanted = [
        "/Volumes/c/s/v/o'brien file.gpkg",
        "/Volumes/c/s/v/o''brien.gpkg",
        "/Volumes/c/s/v/é ü.gpkg",
        "/Volumes/c/s/v/back\\new\\table.gpkg",
        "/Volumes/c/s/v/trailing\\",
    ]
    decoys = [
        "/Volumes/c/s/v/obrien file.gpkg",
        "/Volumes/c/s/v/obrien.gpkg",
        "/Volumes/c/s/v/other.gpkg",
    ]
    df = spark.createDataFrame([(p,) for p in wanted + decoys], "_source_path string")
    got = [r[0] for r in df.where(C.replace_predicate(wanted)).collect()]
    assert sorted(got) == sorted(wanted)


def _gj(path, n):
    feats = [
        {
            "type": "Feature",
            "properties": {"v": i},
            "geometry": {"type": "Point", "coordinates": [i, i]},
        }
        for i in range(n)
    ]
    with open(path, "w") as f:
        json.dump({"type": "FeatureCollection", "features": feats}, f)
    return path


def _events_df(spark, files):
    df = spark.read.format("binaryFile").load(files)
    return C.hash_files(df)


def test_write_batch_empty_is_noop(spark, tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        C, "_stored_hashes", lambda *a, **k: calls.append("stored") or {}
    )
    monkeypatch.setattr(C, "_write_rows", lambda *a, **k: calls.append("write"))
    a = _gj(str(tmp_path / "a.geojson"), 1)
    empty = spark.createDataFrame(
        [],
        "_source_path string, _source_mtime timestamp, _source_hash string, _source_size long",
    )
    assert (
        C.write_batch(empty, "c.s.t", "geojson_gbx", None, "scd2", schema_path=a) == 0
    )
    assert calls == []


def test_write_batch_scd2_appends_new_content_only(spark, tmp_path, monkeypatch):
    a = _gj(str(tmp_path / "a.geojson"), 2)
    b = _gj(str(tmp_path / "b.geojson"), 3)
    events = _events_df(spark, [a, b])
    stored_hash_a = hashlib.sha256(open(a, "rb").read()).hexdigest()
    monkeypatch.setattr(
        C, "_stored_hashes", lambda s, t, paths, mode: {a: {stored_hash_a}}
    )
    written = {}

    def fake_write(rows_df, target, mode, paths, versions):
        written.update(
            mode=mode, paths=paths, n=rows_df.count(), cols=set(rows_df.columns)
        )

    monkeypatch.setattr(C, "_write_rows", fake_write)
    assert (
        C.write_batch(events, "c.s.t", "geojson_gbx", None, "scd2", schema_path=a) == 1
    )
    assert written["paths"] == [b] and written["n"] == 3 and written["mode"] == "scd2"
    assert {"_source_path", "_source_mtime", "_source_hash"} <= written["cols"]


def _frozen_events(spark, files):
    """Materialised hash events (``hash_files`` is lazy and would re-hash on collect)."""
    df = _events_df(spark, files)
    return spark.createDataFrame(df.collect(), df.schema)


@pytest.fixture
def delta_mocks(monkeypatch):
    calls = {"write": 0}
    written = {}
    monkeypatch.setattr(C._listing, "_retry_transient", lambda fn, **k: fn())
    monkeypatch.setattr(
        C,
        "_stored_hashes",
        lambda s, t, paths, mode: written.setdefault("stored_args", (paths, mode))
        and {},
    )

    def fake_write(rows_df, target, mode, paths, versions):
        calls["write"] += 1
        written.update(
            mode=mode,
            paths=paths,
            n=rows_df.count(),
            hashes={r[0] for r in rows_df.select("_source_hash").distinct().collect()},
        )

    monkeypatch.setattr(C, "_write_rows", fake_write)
    return calls, written


def test_write_batch_replace_writes_newest_event_per_path(spark, tmp_path, delta_mocks):
    calls, written = delta_mocks
    a = _gj(str(tmp_path / "a.geojson"), 2)
    b = _gj(str(tmp_path / "b.geojson"), 3)
    real = _frozen_events(spark, [a, b])
    older_a = spark.createDataFrame(
        [(a, datetime(2020, 1, 1), "stale-hash", 1)], real.schema
    )
    hash_a, hash_b = (hashlib.sha256(open(f, "rb").read()).hexdigest() for f in (a, b))
    assert (
        C.write_batch(
            real.union(older_a), "c.s.t", "geojson_gbx", None, "replace", schema_path=a
        )
        == 2
    )
    assert written["mode"] == "replace" and written["paths"] == sorted([a, b])
    assert written["n"] == 5 and written["hashes"] == {hash_a, hash_b}
    assert written["stored_args"][1] == "replace"


def test_write_batch_scd2_keeps_only_newest_content_per_path(
    spark, tmp_path, delta_mocks
):
    calls, written = delta_mocks
    a = _gj(str(tmp_path / "a.geojson"), 2)
    real = _frozen_events(spark, [a])
    older = spark.createDataFrame(
        [(a, datetime(2020, 1, 1), "stale-hash", 1)], real.schema
    )
    assert (
        C.write_batch(
            real.union(older), "c.s.t", "geojson_gbx", None, "scd2", schema_path=a
        )
        == 1
    )
    assert written["hashes"] == {hashlib.sha256(open(a, "rb").read()).hexdigest()}


def test_write_batch_skips_file_rewritten_with_new_size(spark, tmp_path, delta_mocks):
    calls, _ = delta_mocks
    a = _gj(str(tmp_path / "a.geojson"), 2)
    events = _frozen_events(spark, [a])
    _gj(a, 5)  # newer delivery is pending
    assert (
        C.write_batch(events, "c.s.t", "geojson_gbx", None, "scd2", schema_path=a) == 0
    )
    assert calls["write"] == 0


def test_write_batch_skips_file_with_bumped_mtime(spark, tmp_path, delta_mocks):
    calls, _ = delta_mocks
    a = _gj(str(tmp_path / "a.geojson"), 2)
    events = _frozen_events(spark, [a])
    st = os.stat(a)
    os.utime(a, (st.st_atime + 10, st.st_mtime + 10))  # same size, newer mtime
    assert (
        C.write_batch(events, "c.s.t", "geojson_gbx", None, "scd2", schema_path=a) == 0
    )
    assert calls["write"] == 0


def test_write_batch_skips_vanished_file(spark, tmp_path, delta_mocks):
    calls, _ = delta_mocks
    a = _gj(str(tmp_path / "a.geojson"), 2)
    events = _frozen_events(spark, [a])
    os.remove(a)
    assert (
        C.write_batch(
            events, "c.s.t", "geojson_gbx", None, "scd2", schema_path=str(tmp_path)
        )
        == 0
    )
    assert calls["write"] == 0


def test_write_batch_forwards_num_partitions(spark, tmp_path, delta_mocks, monkeypatch):
    a = _gj(str(tmp_path / "a.geojson"), 2)
    seen = {}
    real_read_paths = C.read_paths

    def spy(df, fmt, options=None, **kw):
        seen.update(kw)
        return real_read_paths(df, fmt, options, **kw)

    monkeypatch.setattr(C, "read_paths", spy)
    events = _frozen_events(spark, [a])
    assert (
        C.write_batch(
            events,
            "c.s.t",
            "geojson_gbx",
            None,
            "scd2",
            schema_path=a,
            num_partitions=4,
        )
        == 1
    )
    assert seen["num_partitions"] == 4


class _FakeSpark:
    """Just enough session for the table-exists probe and view DDL."""

    def __init__(self, existing=(), sql_error=None, view_text=None):
        self.existing = set(existing)
        self.sql_error = sql_error
        self.sql_calls = []
        self.view_text = view_text or {}  # view name -> stored query text

    def table(self, name):
        if name not in self.existing:
            raise RuntimeError(
                f"[TABLE_OR_VIEW_NOT_FOUND] The table or view {name} cannot be found."
            )
        return types.SimpleNamespace(columns=["x"])

    def sql(self, stmt):
        self.sql_calls.append(stmt)
        if stmt.startswith("DESCRIBE TABLE EXTENDED "):
            name = stmt[len("DESCRIBE TABLE EXTENDED ") :]
            rows = (
                [("View Text", self.view_text[name])] if name in self.view_text else []
            )
            return types.SimpleNamespace(collect=lambda: rows)
        if self.sql_error:
            raise self.sql_error


EXPECTED_VIEW_SQL = (
    "CREATE VIEW IF NOT EXISTS `my-cat`.`s`.`t_current` AS "
    "WITH readable AS (SELECT v._source_path, v._source_hash, v.__START_AT "
    "FROM `my-cat`.`s`.`t_file_versions` v "
    "LEFT SEMI JOIN (SELECT DISTINCT _source_path, _source_hash FROM `my-cat`.`s`.`t`) s "
    "ON v._source_path = s._source_path AND v._source_hash = s._source_hash), "
    "latest AS (SELECT _source_path, max_by(_source_hash, __START_AT) AS _source_hash "
    "FROM readable GROUP BY _source_path) "
    "SELECT r.* FROM `my-cat`.`s`.`t` r JOIN latest l "
    "ON r._source_path = l._source_path AND r._source_hash = l._source_hash"
)


def test_current_view_shows_each_files_latest_readable_version(spark, tmp_path):
    """A version with no rows (its delivery was set aside, or never written) does
    not hide the file's last readable content; a revert to earlier content is
    current again; a file whose only version has no rows has none."""
    rows, versions = "cv_rows", "cv_rows_file_versions"
    spark.sql(f"DROP VIEW IF EXISTS {rows}_current")
    for t in (rows, versions):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    try:
        spark.sql(f"""CREATE TABLE {rows} USING parquet LOCATION '{tmp_path / "r"}' AS
            SELECT * FROM VALUES
                ('/a', 'h1', 1), ('/a', 'h2', 2), ('/a', 'h2', 3),
                ('/b', 'h1', 4), ('/b', 'h2', 5)
            AS t(_source_path, _source_hash, id)""")
        spark.sql(
            f"""CREATE TABLE {versions} USING parquet LOCATION '{tmp_path / "v"}' AS
            SELECT * FROM VALUES
                ('/a', 'h1', TIMESTAMP'2026-10-17 10:00:00'),
                ('/a', 'h2', TIMESTAMP'2026-10-17 11:00:00'),
                ('/a', 'bad', TIMESTAMP'2026-10-17 12:00:00'),
                ('/b', 'h1', TIMESTAMP'2026-10-17 10:00:00'),
                ('/b', 'h2', TIMESTAMP'2026-10-17 11:00:00'),
                ('/b', 'h1', TIMESTAMP'2026-10-17 12:00:00'),
                ('/c', 'bad', TIMESTAMP'2026-10-17 12:00:00')
            AS t(_source_path, _source_hash, __START_AT)"""
        )
        C._ensure_current_view(spark, rows, versions)
        got = sorted(r["id"] for r in spark.table(f"{rows}_current").collect())
        assert got == [2, 3, 4]
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {rows}_current")
        for t in (rows, versions):
            spark.sql(f"DROP TABLE IF EXISTS {t}")


def test_current_view_sql_quotes_identifiers_and_joins_on_path_and_hash():
    assert (
        C._current_view_sql("my-cat.s.t", "my-cat.s.t_file_versions")
        == EXPECTED_VIEW_SQL
    )
    assert "`a``b`" in C._quote("a`b")


def test_ensure_current_view_creates_when_missing():
    sp = _FakeSpark()
    C._ensure_current_view(sp, "my-cat.s.t", "my-cat.s.t_file_versions")
    assert sp.sql_calls == [EXPECTED_VIEW_SQL]


def test_ensure_current_view_noop_when_present():
    view = "`my-cat`.`s`.`t_current`"
    query = C._current_view_query("my-cat.s.t", "my-cat.s.t_file_versions")
    sp = _FakeSpark(existing={view}, view_text={view: query})
    C._ensure_current_view(sp, "my-cat.s.t", "my-cat.s.t_file_versions")
    assert [c for c in sp.sql_calls if c.startswith("CREATE")] == []


def test_ensure_current_view_replaces_an_older_definition():
    """A view created by an earlier version (open version only) is replaced, so
    an existing pipeline gets the latest-readable-version semantics."""
    view = "`my-cat`.`s`.`t_current`"
    old = "SELECT r.* FROM `my-cat`.`s`.`t` r JOIN v WHERE v.__END_AT IS NULL"
    sp = _FakeSpark(existing={view}, view_text={view: old})
    C._ensure_current_view(sp, "my-cat.s.t", "my-cat.s.t_file_versions")
    (create,) = [c for c in sp.sql_calls if c.startswith("CREATE")]
    assert create.startswith(f"CREATE OR REPLACE VIEW {view} AS WITH readable AS")


def test_ensure_current_view_tolerates_missing_versions_table():
    sp = _FakeSpark(
        sql_error=RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] my-cat.s.t_file_versions")
    )
    C._ensure_current_view(sp, "my-cat.s.t", "my-cat.s.t_file_versions")  # no raise
    assert len(sp.sql_calls) == 1


def test_ensure_current_view_propagates_other_errors():
    sp = _FakeSpark(sql_error=RuntimeError("PERMISSION_DENIED"))
    with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
        C._ensure_current_view(sp, "c.s.t", "c.s.t_file_versions")


def _rows_df(spark_like):
    from unittest import mock

    rows = mock.MagicMock()
    rows.sparkSession = spark_like
    return rows


def test_write_rows_creates_table_non_destructively_then_ensures_view():
    sp = _FakeSpark()
    rows = _rows_df(sp)
    C._write_rows(rows, "c.s.t", "scd2", ["/a"], "c.s.t_file_versions")
    rows.write.mode.assert_called_once_with("append")
    rows.write.mode.return_value.clusterBy.assert_called_once_with("_source_path")
    rows.write.mode.return_value.clusterBy.return_value.saveAsTable.assert_called_once_with(
        "`c`.`s`.`t`"
    )
    assert len(sp.sql_calls) == 1 and "`t_current`" in sp.sql_calls[0]


def test_write_rows_retries_missing_view_on_later_batches():
    sp = _FakeSpark(existing={"`c`.`s`.`t`"})  # table exists, view was never created
    rows = _rows_df(sp)
    C._write_rows(rows, "c.s.t", "scd2", ["/a"], "c.s.t_file_versions")
    rows.write.mode.assert_called_once_with("append")
    assert len(sp.sql_calls) == 1


def test_write_rows_replace_uses_replace_where_and_no_view():
    sp = _FakeSpark(existing={"`c`.`s`.`t`"})
    rows = _rows_df(sp)
    C._write_rows(rows, "c.s.t", "replace", ["/a", "/b"], None)
    rows.write.mode.assert_called_once_with("overwrite")
    rows.write.mode.return_value.option.assert_called_once_with(
        "replaceWhere", "_source_path IN ('/a', '/b')"
    )
    assert sp.sql_calls == []


def test_declare_ingest_validates_before_needing_pipelines():
    with pytest.raises(ValueError, match="catalog.schema.table"):
        C.declare_ingest("schema.table", "/Volumes/c/s/v", "geojson_gbx")
    with pytest.raises(ValueError, match="catalog.schema.table"):
        C.declare_ingest("a.b.c.d", "/Volumes/c/s/v", "geojson_gbx")
    with pytest.raises(ValueError, match="scd2"):
        C.declare_ingest("c.s.t", "/Volumes/c/s/v", "geojson_gbx", on_change="upsert")


# --- object-store URIs -------------------------------------------------------


@pytest.mark.parametrize("uri", ["s3://bucket/a.gpkg", "/vsis3/bucket/a.gpkg"])
def test_hash_batches_rejects_object_store_uri(monkeypatch, uri):
    monkeypatch.setattr(C._listing, "_retry_transient", lambda fn, **k: fn())
    batch = pa.record_batch([pa.array([uri])], names=["path"])
    with pytest.raises(ValueError, match="^hash_files reads Unity Catalog Volume"):
        list(C._hash_batches([batch]))


@pytest.mark.parametrize(
    "uri", ["abfss://c@acct.dfs.core.windows.net/a.gpkg", "/vsiaz/c/a.gpkg"]
)
def test_delivery_check_rejects_object_store_uri(uri):
    e = {
        "_source_path": uri,
        "_source_size": 1,
        C._MTIME_US: 0,
    }
    with pytest.raises(ValueError, match="^declare_ingest reads Unity Catalog Volume"):
        C._delivery_is_current(e)


@pytest.mark.parametrize(
    "kw",
    [
        {"source_dir": "s3://bucket/landing"},
        {"source_dir": "/vsigs/bucket/landing"},
        {"source_dir": "/Volumes/c/s/v", "schema_path": "gs://b/a.geojson"},
    ],
)
def test_declare_ingest_rejects_object_store_uris(kw):
    with pytest.raises(ValueError, match="^declare_ingest reads Unity Catalog Volume"):
        C.declare_ingest("c.s.t", format="geojson_gbx", **kw)


# --- stat tolerance ----------------------------------------------------------


def _stat_event(path, size_delta=0, mtime_delta_us=0):
    st = os.stat(path)
    return {
        "_source_path": str(path),
        "_source_size": st.st_size + size_delta,
        C._MTIME_US: st.st_mtime_ns // 1000 + mtime_delta_us,
    }


@pytest.mark.parametrize(
    "size_delta,mtime_delta_us,current",
    [
        (0, 0, True),
        (0, -500_000, True),  # Auto Loader / FUSE mtimes can be coarser than stat
        (0, 1_000_000, True),
        (0, -1_000_000, True),
        (0, 1_100_000, False),
        (0, -1_100_000, False),
        (1, 0, False),
    ],
)
def test_delivery_is_current_size_exact_mtime_within_a_second(
    tmp_path, monkeypatch, size_delta, mtime_delta_us, current
):
    monkeypatch.setattr(C._listing, "_retry_transient", lambda fn, **k: fn())
    p = tmp_path / "a.bin"
    p.write_bytes(b"hello")
    assert C._delivery_is_current(_stat_event(p, size_delta, mtime_delta_us)) is current


# --- stored hashes + concurrent stat checks ----------------------------------


def test_stored_hashes_semi_joins_the_batch_paths(spark, monkeypatch):
    from pyspark.sql import functions as F

    odd = "/Volumes/c/s/v/o'brien\\x.gpkg"
    spark.createDataFrame(
        [
            (odd, "h1", 1),
            (odd, "h1", 2),
            (odd, "h2", 3),
            ("/v/b", "h9", 1),
            ("/v/c", "h3", 1),
        ],
        "_source_path string, _source_hash string, id int",
    ).createOrReplaceTempView("ii_stored_hashes")

    def no_isin(self, *a):
        raise AssertionError("isin over the batch paths")

    monkeypatch.setattr(type(F.col("x")), "isin", no_isin)
    got = C._stored_hashes(spark, "ii_stored_hashes", [odd, "/v/b", "/v/none"], "scd2")
    assert got == {odd: {"h1", "h2"}, "/v/b": {"h9"}}
    assert C._stored_hashes(spark, "ii_no_such_table", [odd], "scd2") == {}


def test_write_batch_stat_checks_run_concurrently(
    spark, tmp_path, delta_mocks, monkeypatch
):
    import threading

    files = [_gj(str(tmp_path / f"{i}.geojson"), 1) for i in range(4)]
    events = _frozen_events(spark, files)
    barrier = threading.Barrier(len(files), timeout=10)
    monkeypatch.setattr(C, "_delivery_is_current", lambda e: barrier.wait() >= 0)
    assert (
        C.write_batch(
            events, "c.s.t", "geojson_gbx", None, "scd2", schema_path=files[0]
        )
        == 4
    )


# --- idempotent retry --------------------------------------------------------


@pytest.mark.parametrize("mode", ["scd2", "replace"])
def test_write_batch_retry_of_a_written_batch_writes_nothing(
    spark, tmp_path, monkeypatch, mode
):
    monkeypatch.setattr(C._listing, "_retry_transient", lambda fn, **k: fn())
    store = {}  # path -> stored hashes, as the rows table would hold them

    def stored(spark_, target, paths, mode_):
        return {p: set(store[p]) for p in paths if p in store}

    def write(rows_df, target, mode_, paths, versions):
        for p, h in rows_df.select("_source_path", "_source_hash").distinct().collect():
            if mode_ == "replace":
                store[p] = {h}
            else:
                store.setdefault(p, set()).add(h)

    monkeypatch.setattr(C, "_stored_hashes", stored)
    monkeypatch.setattr(C, "_write_rows", write)
    a = _gj(str(tmp_path / "a.geojson"), 2)
    b = _gj(str(tmp_path / "b.geojson"), 3)
    events = _frozen_events(spark, [a, b])
    assert C.write_batch(events, "c.s.t", "geojson_gbx", None, mode, schema_path=a) == 2
    assert C.write_batch(events, "c.s.t", "geojson_gbx", None, mode, schema_path=a) == 0


# --- files DataFrame mtimes --------------------------------------------------


def test_write_batch_builds_files_from_epoch_micros(
    spark, tmp_path, delta_mocks, monkeypatch
):
    from pyspark.sql import SparkSession

    a = _gj(str(tmp_path / "a.geojson"), 2)
    events = _frozen_events(spark, [a])
    want_us = events.selectExpr("unix_micros(_source_mtime)").collect()[0][0]
    seen, captured = [], {}
    real_create = SparkSession.createDataFrame

    def spy_create(self, data, *a, **k):
        seen.extend(list(r) for r in data)
        return real_create(self, data, *a, **k)

    real_read_paths = C.read_paths

    def spy_read_paths(df, *a, **k):
        captured["us"] = df.selectExpr("unix_micros(modificationTime)").collect()[0][0]
        return real_read_paths(df, *a, **k)

    monkeypatch.setattr(SparkSession, "createDataFrame", spy_create)
    monkeypatch.setattr(C, "read_paths", spy_read_paths)
    assert (
        C.write_batch(events, "c.s.t", "geojson_gbx", None, "scd2", schema_path=a) == 1
    )
    assert not [v for row in seen for v in row if isinstance(v, datetime)]
    assert captured["us"] == want_us


# --- identifier quoting --------------------------------------------------------


def test_quote_accepts_plain_and_backticked_names():
    assert C._quote("c.s.t") == "`c`.`s`.`t`"
    assert C._quote("`my-cat`.s.t") == "`my-cat`.`s`.`t`"
    assert C._quote("`a.b`.s.`t``x`") == "`a.b`.`s`.`t``x`"
    for name in ("c.s.t", "`my-cat`.s.t", "`a.b`.s.`t``x`", "a`b"):
        assert C._quote(C._quote(name)) == C._quote(name)
    with pytest.raises(ValueError, match="backtick"):
        C._quote("`unclosed.s.t")


def test_current_view_sql_with_a_backticked_target():
    sql = C._current_view_sql("`my-cat`.s.`t`", "`my-cat`.s.t_file_versions")
    assert sql == EXPECTED_VIEW_SQL


def test_table_identifiers_are_quoted_for_spark_table_and_save_as_table():
    sp = _FakeSpark(existing={"`c`.`s`.`t`"})
    assert C._table_exists(sp, "c.s.t") and C._table_exists(sp, "`c`.s.`t`")
    rows = _rows_df(_FakeSpark())
    C._write_rows(rows, "`c`.s.t", "append", ["/a"], None)
    rows.write.mode.return_value.clusterBy.return_value.saveAsTable.assert_called_once_with(
        "`c`.`s`.`t`"
    )
    rows = _rows_df(sp)
    C._write_rows(rows, "c.s.t", "replace", ["/a"], None)
    rows.write.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(
        "`c`.`s`.`t`"
    )


# --- declare_ingest wiring (pipelines + session faked) ------------------------


class _FakeDP(types.ModuleType):
    """Records the pipeline declarations; decorators keep the decorated function."""

    def __init__(self):
        super().__init__("pyspark.pipelines")
        self.calls, self.fns = [], {}

    def _deco(self, kind, name, **kw):
        def deco(fn):
            self.calls.append((kind, name, kw))
            self.fns[name] = fn
            return fn

        return deco

    def table(self, name=None, **kw):
        return self._deco("table", name, **kw)

    def foreach_batch_sink(self, name=None, **kw):
        return self._deco("sink", name, **kw)

    def append_flow(self, name=None, target=None, **kw):
        return self._deco("append_flow", name, target=target, **kw)

    def create_streaming_table(self, name, **kw):
        self.calls.append(("streaming_table", name, kw))

    def create_auto_cdc_flow(self, **kw):
        self.calls.append(("auto_cdc", kw.get("name"), kw))


class _FakeStreamReader:
    def __init__(self, log):
        self.log = log

    def format(self, f):
        self.log.append(("format", f))
        return self

    def option(self, k, v):
        self.log.append(("option", k, v))
        return self

    def load(self, p):
        self.log.append(("load", p))
        return self

    def select(self, *cols):
        return self

    def table(self, name):
        self.log.append(("table", name))
        return self


@pytest.fixture
def fake_pipeline(monkeypatch):
    import sys

    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    dp, log = _FakeDP(), []
    monkeypatch.setitem(sys.modules, "pyspark.pipelines", dp)
    monkeypatch.setattr(pyspark, "pipelines", dp, raising=False)
    session = types.SimpleNamespace(readStream=_FakeStreamReader(log))
    monkeypatch.setattr(SparkSession, "getActiveSession", lambda: session)
    # Column expressions need a live session; the fake one has none.
    monkeypatch.setattr(F, "col", lambda name: ("col", name))
    return dp, log


def _spy(monkeypatch, name):
    seen = []
    real = getattr(C, name)

    def spy(*a, **k):
        seen.append((a, k))
        return real(*a, **k)

    monkeypatch.setattr(C, name, spy)
    return seen


def test_declare_ingest_scd2_uses_qualified_names_and_one_schema(
    tmp_path, monkeypatch, fake_pipeline
):
    dp, log = fake_pipeline
    d = tmp_path / "landing"
    d.mkdir()
    (d / "0_readme.txt").write_text("x")
    a = _gj(str(d / "a.geojson"), 1)
    schemas = _spy(monkeypatch, "path_schema")
    monkeypatch.setattr(C, "hash_files", lambda df: df)
    batches = []
    monkeypatch.setattr(C, "write_batch", lambda *a, **k: batches.append((a, k)))

    C.declare_ingest("main.geo.t", str(d), "geojson_gbx")

    tables = [n for kind, n, _ in dp.calls if kind in ("table", "streaming_table")]
    assert tables == ["main.geo.t_file_events", "main.geo.t_file_versions"]
    (cdc,) = [kw for kind, _, kw in dp.calls if kind == "auto_cdc"]
    assert cdc["target"] == "main.geo.t_file_versions"
    assert cdc["source"] == "main.geo.t_file_events"
    assert cdc["sequence_by"] == ("col", "_source_mtime")
    (flow,) = [(n, kw) for kind, n, kw in dp.calls if kind == "append_flow"]
    assert flow == ("t_rows_flow", {"target": "t_rows_sink"})

    dp.fns["main.geo.t_file_events"]()
    assert ("option", "pathGlobFilter", "*.{geojson,GEOJSON}") in log
    assert ("load", str(d)) in log
    dp.fns["t_rows_flow"]()
    assert ("table", "`main`.`geo`.`t_file_events`") in log

    for batch_id in range(2):  # every micro-batch reuses the declared schema
        dp.fns["t_rows_sink"](object(), batch_id)
    assert len(schemas) == 1 and schemas[0][1]["glob"] == "*.{geojson,GEOJSON}"
    resolved = P_path_schema(a)
    for args, kw in batches:
        assert args[1] == "main.geo.t" and kw["versions"] == "main.geo.t_file_versions"
        assert kw["schema"] == resolved


def P_path_schema(path):
    from databricks.labs.gbx.ds.paths import path_schema

    return path_schema("geojson_gbx", None, path)


def test_declare_ingest_append_and_explicit_glob(tmp_path, monkeypatch, fake_pipeline):
    dp, log = fake_pipeline
    a = _gj(str(tmp_path / "zones_a.geojson"), 1)
    reads = []
    monkeypatch.setattr(C, "read_paths", lambda *a, **k: reads.append((a, k)))

    C.declare_ingest(
        "`my-cat`.geo.t", str(tmp_path), "geojson", on_change="append", glob="zones_*"
    )

    assert [(kind, n) for kind, n, _ in dp.calls] == [("table", "`my-cat`.geo.t")]
    dp.fns["`my-cat`.geo.t"]()
    assert ("option", "pathGlobFilter", "zones_*") in log
    ((_, kw),) = reads
    assert kw["schema"] == P_path_schema(a) and "schema_path" not in kw


def test_ingest_names_and_default_glob():
    got = C._ingest_setup("c.s.t", "/Volumes/c/s/v", "lidar", "replace", None, None)
    assert got["events"] == "c.s.t_file_events"
    assert got["versions"] == "c.s.t_file_versions"
    assert got["rows"] == "c.s.t" and got["glob"] == "*.{las,laz,LAS,LAZ}"
    assert got["schema_src"] == "/Volumes/c/s/v"
    odd = C._ingest_setup(
        "`a-b`.s.t", "/V", "gpkg_gbx", "scd2", "x_*.gpkg", "/V/x_1.gpkg"
    )
    assert odd["events"] == "`a-b`.s.t_file_events" and odd["glob"] == "x_*.gpkg"
    assert odd["schema_src"] == "/V/x_1.gpkg"


def test_ingest_default_glob_follows_the_pmtiles_source_option():
    def glob(options):
        setup = C._ingest_setup("c.s.t", "/V", "pmtiles", "scd2", None, None, options)
        return setup["glob"]

    assert glob({"source": "archive"}) == "*.{pmtiles,PMTILES}"
    assert glob({"source": "raster"}) == "*.tif"
    with pytest.raises(ValueError, match="source"):
        glob(None)


# --- on_error: quarantine ------------------------------------------------------


def test_on_error_modes_constant():
    assert C.ON_ERROR_MODES == ("quarantine", "fail")


def test_write_batch_checks_on_error(spark, tmp_path, delta_mocks):
    a = _gj(str(tmp_path / "a.geojson"), 1)
    events = _frozen_events(spark, [a])
    with pytest.raises(ValueError, match="on_error must be one of"):
        C.write_batch(events, "c.s.t", "geojson_gbx", None, "scd2", on_error="skip")
    with pytest.raises(ValueError, match="needs scratch_dir"):
        C.write_batch(
            events, "c.s.t", "geojson_gbx", None, "scd2", on_error="quarantine"
        )


class _Tables:
    """The rows table and <t>_bad_records in memory, behind the Delta writers:
    stored hashes come from what was written, bad records are appended through
    the real de-duplication, and each writer can be made to fail once."""

    def __init__(self, spark):
        self.spark = spark
        self.rows = {}  # path -> set of stored hashes
        self.row_writes = []  # (mode, paths, row count) per rows write
        self.bad = []  # appended bad records (dicts)
        self.created = []  # schemas the rows table was created with
        self.row_schemas = []  # schema of each rows write
        self.fail_rows = self.fail_bad = 0

    def stored_hashes(self, spark, target, paths, mode):
        return {p: set(self.rows[p]) for p in paths if p in self.rows}

    def write_rows(self, rows_df, target, mode, paths, versions):
        if self.fail_rows:
            self.fail_rows -= 1
            raise RuntimeError("rows write failed")
        self.row_schemas.append(rows_df.schema)
        got = rows_df.select("_source_path", "_source_hash").distinct().collect()
        for p in paths if mode == "replace" else []:
            self.rows.pop(p, None)
        for p, h in got:
            self.rows.setdefault(p, set()).add(h)
        self.row_writes.append((mode, list(paths), rows_df.count()))

    def existing_bad(self, spark, table, paths):
        if not self.bad:
            return None
        keys = [tuple(r[k] for k in C._BAD_RECORD_KEY) for r in self.bad]
        return spark.createDataFrame(
            keys,
            "_source_path string, _source_hash string, record_index string, "
            "error_class string",
        )

    def append_bad(self, bad_df, table):
        assert table == "`c`.`s`.`t_bad_records`"
        if self.fail_bad:
            self.fail_bad -= 1
            raise RuntimeError("bad-records write failed")
        self.bad.extend(r.asDict() for r in bad_df.collect())

    def quarantined(self, spark, target, paths):
        out = {}
        for r in self.bad:
            if r["error_class"] == "NothingReadable" and r["_source_path"] in paths:
                out.setdefault(r["_source_path"], set()).add(r["_source_hash"])
        return out


@pytest.fixture
def tables(spark, monkeypatch):
    monkeypatch.setattr(C._listing, "_retry_transient", lambda fn, **k: fn())
    t = _Tables(spark)
    monkeypatch.setattr(C, "_stored_hashes", t.stored_hashes)
    monkeypatch.setattr(C, "_write_rows", t.write_rows)
    monkeypatch.setattr(C, "_existing_bad_records", t.existing_bad)
    monkeypatch.setattr(C, "_append_bad_records", t.append_bad)
    monkeypatch.setattr(C, "_quarantined_hashes", t.quarantined)
    monkeypatch.setattr(
        C,
        "_ensure_target",
        lambda spark, target, schema: t.created.append(schema),
        raising=False,
    )
    return t


_RING = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0]]]}  # not closed: no WKB


def _mixed_landing(tmp_path):
    """good.geojson (3 features), mixed.geojson (feature 1's ring is not closed:
    unreadable; feature 2 has no geometry: data) and bad.geojson (not GeoJSON)."""
    landing = tmp_path / "landing"
    landing.mkdir()
    good = _gj(str(landing / "good.geojson"), 3)
    mixed = landing / "mixed.geojson"
    feats = json.loads(open(good).read())
    feats["features"][1]["geometry"] = _RING
    feats["features"][2]["geometry"] = None
    mixed.write_text(json.dumps(feats))
    bad = landing / "bad.geojson"
    bad.write_bytes(b"\x00not a geospatial file" * 20)
    return landing, good, str(mixed), str(bad)


def _quarantine(events, mode, landing, good, **kw):
    return C.write_batch(
        events,
        "c.s.t",
        "geojson_gbx",
        None,
        mode,
        schema_path=good,
        on_error="quarantine",
        scratch_dir=str(landing),
        **kw,
    )


@pytest.mark.parametrize("mode", ["scd2", "replace"])
def test_write_batch_quarantine_writes_the_rows_and_sets_bad_records_aside(
    spark, tmp_path, tables, mode
):
    landing, good, mixed, bad = _mixed_landing(tmp_path)
    events = _frozen_events(spark, [good, mixed, bad])
    by_path = {r["_source_path"]: r for r in events.collect()}
    assert _quarantine(events, mode, landing, good) == 3
    # The rows: every feature (a NULL geometry is data) but mixed's unreadable one.
    ((_, paths, n),) = tables.row_writes
    assert n == 3 + 2
    # replace keeps bad.geojson's earlier rows: set aside entirely, it is not
    # replaced; scd2 is append-only.
    want = [good, mixed] if mode == "replace" else sorted([good, mixed, bad])
    assert sorted(paths) == sorted(want)
    recs = sorted(tables.bad, key=lambda r: (r["_source_path"], r["error_class"]))
    assert [(r["_source_path"], r["record_index"], r["error_class"]) for r in recs] == [
        (bad, None, "NothingReadable"),
        (bad, None, "pyogrio.errors.DataSourceError"),
        (mixed, "feature=1", "UnreadableGeometry"),
    ]
    for r in recs:  # the delivery's hash and size, joined on the driver
        assert r["_source_hash"] == by_path[r["_source_path"]]["_source_hash"]
        assert r["_source_size"] == by_path[r["_source_path"]]["_source_size"]
        assert r["quarantined_at"] is not None
        assert list(r) == list(Q.BAD_RECORDS_COLUMNS)
    # The micro-batch's staging folder is gone, and so is its hidden container.
    assert not os.path.exists(landing / ".gbx_scratch")


def test_write_batch_quarantine_without_bad_records_writes_no_side_table(
    spark, tmp_path, tables
):
    landing, good, _, _ = _mixed_landing(tmp_path)
    _quarantine(_frozen_events(spark, [good]), "scd2", landing, good)
    assert tables.row_writes[0][2] == 3 and tables.bad == []
    assert sorted(os.listdir(landing)) == [
        "bad.geojson",
        "good.geojson",
        "mixed.geojson",
    ]


@pytest.mark.parametrize("fails", ["bad", "rows"])
def test_a_retried_batch_loses_and_duplicates_no_bad_record(
    spark, tmp_path, tables, fails
):
    """Bad records are appended before the rows, skipping the ones already
    there: a failure of either write and the retry leave each bad record once
    and every row written."""
    landing, good, mixed, bad = _mixed_landing(tmp_path)
    events = _frozen_events(spark, [good, mixed, bad])
    if fails == "bad":
        tables.fail_bad = 1
    else:
        tables.fail_rows = 1
    with pytest.raises(RuntimeError, match="write failed"):
        _quarantine(events, "scd2", landing, good)
    assert tables.row_writes == []  # the rows are written last
    assert len(tables.bad) == (0 if fails == "bad" else 3)
    _quarantine(events, "scd2", landing, good)  # the streaming retry
    keys = [
        (r["_source_path"], r["record_index"], r["error_class"]) for r in tables.bad
    ]
    assert len(keys) == len(set(keys)) == 3
    assert set(tables.rows) == {good, mixed}


def test_an_identical_redelivery_of_a_file_set_aside_entirely_is_skipped(
    spark, tmp_path, tables
):
    landing, good, _, bad = _mixed_landing(tmp_path)
    events = _frozen_events(spark, [bad])
    assert _quarantine(events, "scd2", landing, good) == 1
    assert _quarantine(events, "scd2", landing, good) == 0  # same bytes again
    assert len(tables.bad) == 2  # its record and NothingReadable, once
    open(bad, "ab").write(b"more garbage")  # new content: read (and set aside) again
    assert _quarantine(_frozen_events(spark, [bad]), "scd2", landing, good) == 1
    assert len(tables.bad) == 4


def test_write_batch_stages_in_the_scratch_dir_it_is_given(
    spark, tmp_path, tables, monkeypatch
):
    landing, good, _, bad = _mixed_landing(tmp_path)
    other = tmp_path / "writable"
    other.mkdir()
    seen = {}

    def spy(rows_df, *a):
        seen["other"] = os.listdir(other / ".gbx_scratch")
        seen["landing"] = os.path.exists(landing / ".gbx_scratch")
        tables.write_rows(rows_df, *a)

    monkeypatch.setattr(C, "_write_rows", spy)
    C.write_batch(
        _frozen_events(spark, [good, bad]),
        "c.s.t",
        "geojson_gbx",
        None,
        "scd2",
        schema_path=good,
        on_error="quarantine",
        scratch_dir=str(other),
    )
    assert len(seen["other"]) == 1 and seen["landing"] is False
    assert not os.path.exists(other / ".gbx_scratch")


def test_a_scratch_dir_that_cannot_be_written_says_what_to_do(tmp_path):
    blocker = tmp_path / "landing"
    blocker.write_text("a file, not a folder")
    with pytest.raises(PermissionError, match="scratch_dir="):
        C._new_batch_scratch(str(blocker), "c.s.t")


def test_write_batch_fail_raises_naming_the_file(spark, tmp_path, tables):
    landing, good, _, bad = _mixed_landing(tmp_path)
    events = _frozen_events(spark, [good, bad])
    with pytest.raises(Exception, match=re.escape(bad)):
        C.write_batch(events, "c.s.t", "geojson_gbx", None, "scd2", schema_path=good)
    assert tables.bad == [] and tables.row_writes == []
    assert not os.path.exists(landing / ".gbx_scratch")  # fail mode stages nothing


def test_new_bad_records_skips_what_the_table_holds(spark):
    """The anti-join matches a NULL record_index too."""
    cols = (
        "_source_path string, _source_mtime timestamp, _source_hash string, "
        "_source_size long, record_index string, error_class string, "
        "error_message string, raw string, quarantined_at timestamp"
    )
    bad = spark.createDataFrame(
        [
            ("/a", None, "h", 1, None, "E", "m", None, None),
            ("/a", None, "h", 1, "feature=2", "E", "m", None, None),
            ("/a", None, "h2", 1, None, "E", "m", None, None),
        ],
        cols,
    )
    existing = spark.createDataFrame(
        [("/a", "h", None, "E")],
        "_source_path string, _source_hash string, record_index string, "
        "error_class string",
    )
    got = C._new_bad_records(bad, existing).collect()
    assert sorted((r["_source_hash"], r["record_index"] or "") for r in got) == [
        ("h", "feature=2"),
        ("h2", ""),
    ]
    assert C._new_bad_records(bad, None) is bad


_BAD_RECORDS_DDL = (
    "_source_path string, _source_mtime timestamp, _source_hash string, "
    "_source_size long, record_index string, error_class string, "
    "error_message string, raw string, quarantined_at timestamp"
)


@pytest.fixture
def bad_records_view(spark):
    """``t_bad_records`` (the bad-records table of target ``t``) as a temp
    view of the given rows; dropped after the test."""
    spark.catalog.dropTempView("t_bad_records")

    def make(rows):
        df = spark.createDataFrame(rows, _BAD_RECORDS_DDL)
        df.createOrReplaceTempView("t_bad_records")

    yield make
    spark.catalog.dropTempView("t_bad_records")


def test_existing_bad_records_reads_the_keys_held_for_the_paths(
    spark, bad_records_view
):
    """The real query: the record keys the table holds for the given paths,
    once each, a NULL record_index kept; None while there is no table."""
    table = C._bad_records_table("t")
    assert C._existing_bad_records(spark, table, ["/a"]) is None
    nothing = Q.NOTHING_READABLE
    bad_records_view(
        [
            ("/a", None, "h1", 1, None, nothing, "m", None, None),
            ("/a", None, "h1", 1, "feature=2", "E", "m", None, None),
            ("/a", None, "h1", 1, "feature=2", "E", "m again", None, None),
            ("/a", None, "h2", 1, None, "E", "m", None, None),
            ("/b", None, "h3", 1, None, "E", "m", None, None),
        ]
    )
    got = C._existing_bad_records(spark, table, ["/a"]).collect()
    assert sorted((tuple(r) for r in got), key=lambda k: (k[1], k[2] or "")) == [
        ("/a", "h1", None, nothing),
        ("/a", "h1", "feature=2", "E"),
        ("/a", "h2", None, "E"),
    ]


def test_quarantined_hashes_reads_the_deliveries_set_aside_whole(
    spark, bad_records_view
):
    """The real query: per given path, the hashes with a NothingReadable
    record; other records and other paths are not counted."""
    assert C._quarantined_hashes(spark, "t", ["/a"]) == {}
    nothing = Q.NOTHING_READABLE
    bad_records_view(
        [
            ("/a", None, "h1", 1, None, nothing, "m", None, None),
            ("/a", None, "h2", 1, "feature=1", Q.UNREADABLE_GEOMETRY, "m", None, None),
            ("/b", None, "h3", 1, None, nothing, "m", None, None),
            ("/c", None, "h4", 1, None, nothing, "m", None, None),
        ]
    )
    assert C._quarantined_hashes(spark, "t", ["/a", "/b"]) == {
        "/a": {"h1"},
        "/b": {"h3"},
    }
    assert C._quarantined_hashes(spark, "t", []) == {}


def test_bad_records_df_keeps_a_record_written_twice_once(spark, tmp_path):
    """A task retry writes its records again: the driver keeps one copy."""
    scratch = str(tmp_path / "s")
    for _ in range(2):
        bad = Q.BadRecords(scratch)
        bad.add("/v/a.gpkg", 1_000_000, "fid=7", "RecordsNotRead", "lost", None)
        bad.add("/v/a.gpkg", 1_000_000, None, "RecordsNotRead", "1 of 9", None)
        bad.close()
    files = spark.createDataFrame(
        [("/v/a.gpkg", 1_000_000, "h1", 42)],
        "path string, us long, _source_hash string, _source_size long",
    ).selectExpr(
        "path",
        "timestamp_micros(us) AS modificationTime",
        "_source_hash",
        "_source_size",
    )
    rows = C._bad_records_df(spark, scratch, files).collect()
    assert sorted((r["record_index"] or "") for r in rows) == ["", "fid=7"]
    assert {(r["_source_hash"], r["_source_size"]) for r in rows} == {("h1", 42)}
    assert C._bad_records_df(spark, str(tmp_path / "never_created"), files) is None


def test_declare_ingest_passes_on_error_to_the_sink(
    tmp_path, monkeypatch, fake_pipeline
):
    dp, _ = fake_pipeline
    _gj(str(tmp_path / "a.geojson"), 1)
    batches = []
    monkeypatch.setattr(C, "write_batch", lambda *a, **k: batches.append(k))
    monkeypatch.setattr(C, "hash_files", lambda df: df)

    C.declare_ingest("main.geo.t", str(tmp_path), "geojson_gbx")  # quarantine
    dp.fns["t_rows_sink"](object(), 0)
    C.declare_ingest("main.geo.u", str(tmp_path), "geojson_gbx", on_error="fail")
    dp.fns["u_rows_sink"](object(), 0)
    C.declare_ingest(
        "main.geo.v", str(tmp_path), "geojson_gbx", scratch_dir="/Volumes/c/s/scratch"
    )
    dp.fns["v_rows_sink"](object(), 0)
    assert [(k["on_error"], k["scratch_dir"]) for k in batches] == [
        ("quarantine", str(tmp_path)),
        ("fail", None),
        ("quarantine", "/Volumes/c/s/scratch"),
    ]
    with pytest.raises(ValueError, match="on_error must be one of"):
        C.declare_ingest("main.geo.t", str(tmp_path), "geojson_gbx", on_error="skip")
    with pytest.raises(ValueError, match="^declare_ingest reads Unity Catalog Volume"):
        C.declare_ingest(
            "main.geo.t", str(tmp_path), "geojson_gbx", scratch_dir="s3://b/scratch"
        )


@pytest.mark.parametrize(
    "on_error, read_mode", [("quarantine", "skip"), ("fail", "fail")]
)
def test_declare_ingest_append_skips_bad_records(
    tmp_path, monkeypatch, fake_pipeline, on_error, read_mode
):
    """append is a plain streaming table: no side table, bad records are skipped."""
    dp, _ = fake_pipeline
    _gj(str(tmp_path / "a.geojson"), 1)
    reads = []
    monkeypatch.setattr(C, "read_paths", lambda *a, **k: reads.append(k))
    C.declare_ingest(
        "main.geo.t",
        str(tmp_path),
        "geojson_gbx",
        on_change="append",
        on_error=on_error,
    )
    dp.fns["main.geo.t"]()
    assert reads[0]["on_error"] == read_mode


def test_ingest_setup_names_the_bad_records_table():
    got = C._ingest_setup("`a-b`.s.t", "/V", "gpkg_gbx", "scd2", None, None)
    assert got["bad_records"] == "`a-b`.s.t_bad_records"


# --- fix round 2: staging namespace, Volume paths, dedupe, NOT NULL, views ------


def test_each_target_stages_in_its_own_namespace(tmp_path):
    a = C._new_batch_scratch(str(tmp_path), "main.geo.t")
    b = C._new_batch_scratch(str(tmp_path), "main.geo.u")
    again = C._new_batch_scratch(str(tmp_path), "`main`.`geo`.`t`")
    container = tmp_path / ".gbx_scratch"
    for path in (a, b, again):
        assert os.path.dirname(os.path.dirname(path)) == str(container)
    key_a, key_b = os.path.basename(os.path.dirname(a)), os.path.basename(
        os.path.dirname(b)
    )
    assert key_a != key_b and key_a == os.path.basename(os.path.dirname(again))
    assert key_a.startswith("main.geo.t-") and not key_a.startswith(".")


def test_staging_gc_removes_only_its_own_day_old_batches(tmp_path):
    """Two sinks can share a scratch_dir (the default source_dir, or a shared
    folder): one never removes the other's batches, and its own only once they
    are a day old."""
    import time as _time

    mine = os.path.dirname(C._new_batch_scratch(str(tmp_path), "main.geo.t"))
    theirs = os.path.dirname(C._new_batch_scratch(str(tmp_path), "main.geo.u"))
    now = _time.time()
    made = {}
    for name, parent, age_h in (
        ("old", mine, 30),
        ("recent", mine, 2),
        ("their_old", theirs, 30),
        ("writer", str(tmp_path / ".gbx_scratch"), 30),
    ):
        d = os.path.join(parent, name)
        os.makedirs(d)
        os.utime(d, (now - age_h * 3600, now - age_h * 3600))
        made[name] = d
    C._new_batch_scratch(str(tmp_path), "main.geo.t")
    assert not os.path.exists(made["old"])
    for name in ("recent", "their_old"):
        assert os.path.exists(made[name]), name


def test_staging_gc_also_removes_day_old_unnamespaced_folders(tmp_path):
    """Folders directly under .gbx_scratch (staging from before namespaces, or
    a writer's orphan) are removed a day old too; namespaces never are."""
    import time as _time
    import uuid as _uuid

    container = tmp_path / ".gbx_scratch"
    theirs = os.path.dirname(C._new_batch_scratch(str(tmp_path), "main.geo.u"))
    now = _time.time()
    made = {}
    for name, parent, age_h in (
        ("legacy_old", str(container), 30),
        ("legacy_recent", str(container), 2),
        ("their_old", theirs, 30),
    ):
        d = os.path.join(
            parent, _uuid.uuid4().hex if name.startswith("legacy") else name
        )
        os.makedirs(d)
        os.utime(d, (now - age_h * 3600, now - age_h * 3600))
        made[name] = d
    os.utime(theirs, (now - 30 * 3600, now - 30 * 3600))  # an idle namespace
    C._new_batch_scratch(str(tmp_path), "main.geo.t")
    assert not os.path.exists(made["legacy_old"])
    for name in ("legacy_recent", "their_old"):
        assert os.path.exists(made[name]), name


def test_scratch_dir_must_be_a_volume_path():
    with pytest.raises(ValueError, match="/Volumes"):
        C._ingest_setup(
            "c.s.t",
            "/Volumes/c/s/v",
            "gpkg_gbx",
            "scd2",
            None,
            None,
            None,
            "quarantine",
            "/local_disk0/tmp/scratch",
        )
    got = C._ingest_setup(
        "c.s.t",
        "/Volumes/c/s/v",
        "gpkg_gbx",
        "scd2",
        None,
        None,
        None,
        "quarantine",
        "dbfs:/Volumes/c/s/w/scratch",
    )
    assert got["scratch_dir"] == "/Volumes/c/s/w/scratch"


def test_bad_records_df_dedupes_on_the_record_key(spark, tmp_path):
    """Two attempts can describe the same record differently (the rows read
    before an error, the warnings seen): it is still one record."""
    scratch = str(tmp_path / "s")
    for message in ("lost after 10 rows", "lost after 12 rows"):
        bad = Q.BadRecords(scratch)
        bad.add("/v/a.gpkg", 1_000_000, None, "OSError", message, None)
        bad.close()
    files = spark.createDataFrame(
        [("/v/a.gpkg", 1_000_000, "h1", 42)],
        "path string, us long, _source_hash string, _source_size long",
    ).selectExpr(
        "path",
        "timestamp_micros(us) AS modificationTime",
        "_source_hash",
        "_source_size",
    )
    assert C._bad_records_df(spark, scratch, files).count() == 1


def test_quarantine_creates_the_rows_table_from_the_readers_schema(
    spark, tmp_path, tables
):
    """Fail mode writes the reader's rows, which creates the table from the
    reader schema. Quarantine mode stages the batch (Parquet reads back
    all-nullable), so it creates the table first, from that same reader schema,
    and then appends the staged copy: same columns, types and order. (What the
    catalog persists for nullability is its own business; Databricks stores
    nullable columns in both modes.)"""
    import laspy

    landing = tmp_path / "landing"
    landing.mkdir()
    las = laspy.LasData(laspy.LasHeader(point_format=3, version="1.2"))
    las.x, las.y, las.z = [1.0, 2.0], [1.0, 2.0], [1.0, 2.0]
    path = str(landing / "a.las")
    las.write(path)
    args = ("c.s.t", "lidar_gbx", {"mode": "metadata"}, "append")

    C.write_batch(
        _frozen_events(spark, [path]), *args, schema_path=path, on_error="fail"
    )
    (fail_schema,) = tables.row_schemas
    assert tables.created == []  # fail mode creates it through the rows write
    # the comparison below tells the reader schema from the staged one only
    # because the reader has a NOT NULL column
    assert not fail_schema["path"].nullable

    tables.row_schemas.clear()
    C.write_batch(
        _frozen_events(spark, [path]),
        *args,
        schema_path=path,
        on_error="quarantine",
        scratch_dir=str(landing),
    )
    (created,) = tables.created
    (staged,) = tables.row_schemas
    assert created == fail_schema
    assert [(f.name, f.dataType) for f in staged] == [
        (f.name, f.dataType) for f in created
    ]
    assert {"_source_path", "_source_hash"} <= set(created.fieldNames())
    assert len(tables.row_writes) == 2  # the staged append went through


def test_ensure_target_creates_an_empty_table_with_the_schema():
    from unittest import mock

    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField("path", StringType(), False)])
    sp = mock.MagicMock()
    sp.table.side_effect = RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] c.s.t")
    C._ensure_target(sp, "c.s.t", schema)
    sp.createDataFrame.assert_called_once_with([], schema)
    w = sp.createDataFrame.return_value.write
    w.mode.assert_called_once_with("append")
    w.mode.return_value.clusterBy.assert_called_once_with("_source_path")
    w.mode.return_value.clusterBy.return_value.saveAsTable.assert_called_once_with(
        "`c`.`s`.`t`"
    )
    sp = mock.MagicMock()  # exists: nothing is created
    C._ensure_target(sp, "c.s.t", schema)
    sp.createDataFrame.assert_not_called()
