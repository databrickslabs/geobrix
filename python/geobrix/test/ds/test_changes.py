"""on_change helpers: hashing, write planning, batch writes (Delta parts mocked)."""

import hashlib
import json
from datetime import datetime, timedelta, timezone

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


def test_plan_writes_scd2_each_distinct_content_once_same_batch():
    out = C.plan_writes(
        [ev("/a", "h2", 3), ev("/a", "h3", 4), ev("/a", "h2", 5)],
        {"/a": {"h1"}},
        "scd2",
    )
    assert sorted((e["_source_hash"], e["_source_mtime"].minute) for e in out) == [
        ("h2", 3),
        ("h3", 4),
    ]


def test_plan_writes_rejects_unknown_mode():
    with pytest.raises(ValueError, match="scd2"):
        C.plan_writes([], {}, "upsert")


def test_replace_predicate_escapes_quotes():
    pred = C.replace_predicate(
        ["/Volumes/c/s/v/o'brien file.gpkg", "/Volumes/c/s/v/é.gpkg"]
    )
    assert (
        pred
        == "_source_path IN ('/Volumes/c/s/v/o''brien file.gpkg', '/Volumes/c/s/v/é.gpkg')"
    )


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
