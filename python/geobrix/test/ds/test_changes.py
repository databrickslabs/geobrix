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
        "c.s.t"
    )
    assert len(sp.sql_calls) == 1 and "`t_current`" in sp.sql_calls[0]


def test_write_rows_retries_missing_view_on_later_batches():
    sp = _FakeSpark(existing={"c.s.t"})  # table exists, view was never created
    rows = _rows_df(sp)
    C._write_rows(rows, "c.s.t", "scd2", ["/a"], "c.s.t_file_versions")
    rows.write.mode.assert_called_once_with("append")
    assert len(sp.sql_calls) == 1


def test_write_rows_replace_uses_replace_where_and_no_view():
    sp = _FakeSpark(existing={"c.s.t"})
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
