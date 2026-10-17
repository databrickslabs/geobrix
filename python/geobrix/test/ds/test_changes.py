"""on_change helpers: hashing, write planning, batch writes (Delta parts mocked)."""

import hashlib
import json
import os
import types
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

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

    def __init__(self, existing=(), sql_error=None):
        self.existing = set(existing)
        self.sql_error = sql_error
        self.sql_calls = []

    def table(self, name):
        if name not in self.existing:
            raise RuntimeError(
                f"[TABLE_OR_VIEW_NOT_FOUND] The table or view {name} cannot be found."
            )
        return types.SimpleNamespace(columns=["x"])

    def sql(self, stmt):
        self.sql_calls.append(stmt)
        if self.sql_error:
            raise self.sql_error


EXPECTED_VIEW_SQL = (
    "CREATE VIEW IF NOT EXISTS `my-cat`.`s`.`t_current` AS SELECT r.* FROM `my-cat`.`s`.`t` r "
    "JOIN `my-cat`.`s`.`t_file_versions` v ON r._source_path = v._source_path "
    "AND r._source_hash = v._source_hash WHERE v.__END_AT IS NULL"
)


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
    sp = _FakeSpark(existing={"`my-cat`.`s`.`t_current`"})
    C._ensure_current_view(sp, "my-cat.s.t", "my-cat.s.t_file_versions")
    assert sp.sql_calls == []


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
    with pytest.raises(ValueError, match="Unity Catalog Volume / FUSE paths"):
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
    with pytest.raises(ValueError, match="Unity Catalog Volume / FUSE paths"):
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
    with pytest.raises(ValueError, match="Unity Catalog Volume / FUSE paths"):
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
    assert ("option", "pathGlobFilter", "*.{geojson,json,GEOJSON,JSON}") in log
    assert ("load", str(d)) in log
    dp.fns["t_rows_flow"]()
    assert ("table", "`main`.`geo`.`t_file_events`") in log

    for batch_id in range(2):  # every micro-batch reuses the declared schema
        dp.fns["t_rows_sink"](object(), batch_id)
    assert (
        len(schemas) == 1 and schemas[0][1]["glob"] == "*.{geojson,json,GEOJSON,JSON}"
    )
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
