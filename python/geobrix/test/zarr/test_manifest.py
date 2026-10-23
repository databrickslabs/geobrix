"""The per-store Delta manifest: schema, snapshots, and the compare-and-swap commit.

The commit tests run against local OSS Delta, which has no row-level concurrency: every
concurrent MERGE conflicts there. So nothing here expects two disjoint writers to land
without a retry; real concurrency is exercised on Databricks.
"""

import dataclasses
import time
import uuid
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest
from pyspark.sql import functions as F

from databricks.labs.gbx.zarr import _manifest as M
from databricks.labs.gbx.zarr.errors import (
    ZarrError,
    ZarrOwnershipError,
    ZarrVersionUnavailable,
    ZarrWriteConflict,
)

PROPS = M.StoreProps(
    kind="managed",
    root="/tmp/root",
    overlay_root=None,
    writable=True,
    file_mode="fuse",
    retain="7 days",
)


def _rows(*specs):
    cols = {
        k: []
        for k in (
            "kind",
            "array",
            "key",
            "shard_key",
            "file",
            "offset",
            "length",
            "checksum",
            "origin",
            "identity",
            "write_id",
            "tag",
            "meta",
            "op",
            "expected_write_id",
        )
    }
    for s in specs:
        for k in cols:
            cols[k].append(s.get(k))
    schema = pa.schema(
        [(k, pa.int64() if k in ("offset", "length") else pa.string()) for k in cols]
    )
    return pa.Table.from_pydict(cols, schema=schema)


def _chunk(key, wid, expected=None, file="data/t/a.zpack"):
    return dict(
        kind="chunk",
        array="t2m",
        key=key,
        shard_key="c/0/0/0",
        file=file,
        offset=0,
        length=10,
        checksum="00",
        origin="gbx",
        identity=None,
        write_id=wid,
        tag=None,
        meta=None,
        op="upsert",
        expected_write_id=expected,
    )


def _delete(key, expected):
    row = _chunk(key, uuid.uuid4().hex, expected=expected)
    row.update(op="delete", file=None, offset=None, length=None, checksum=None)
    return row


def _keys(table):
    return sorted((r["kind"], r["array"], r["key"]) for r in table.to_pylist())


def _live(spark, table):
    return M.read_rows(spark, M.snapshot(spark, table), kinds=["chunk"]).to_pylist()


@pytest.fixture
def table(spark_delta):
    name = f"zm_{uuid.uuid4().hex[:8]}"
    M.create_manifest(spark_delta, name, PROPS)
    yield name
    spark_delta.sql(f"DROP TABLE IF EXISTS {name}")


# --- the brief's five -------------------------------------------------------------


def test_create_sets_properties_and_refuses_existing(spark_delta, table):
    props = M.read_props(spark_delta, table)
    assert props == PROPS
    tp = {
        r["key"]: r["value"]
        for r in spark_delta.sql(f"SHOW TBLPROPERTIES {table}").collect()
    }
    assert (
        tp["delta.enableChangeDataFeed"] == "true"
        and tp["delta.deletedFileRetentionDuration"] == "interval 7 days"
    )
    with pytest.raises(ZarrOwnershipError):
        M.create_manifest(spark_delta, table, PROPS)


def test_commit_insert_then_cas_update(spark_delta, table):
    w1, w2 = uuid.uuid4().hex, uuid.uuid4().hex
    v1 = M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w1)),
        write_id=w1,
        meta={"mode": "create"},
    )
    v2 = M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w2, expected=w1, file="data/t/b.zpack")),
        write_id=w2,
        meta={"mode": "update"},
    )
    assert v2 == v1 + 1
    live = M.read_rows(
        spark_delta, M.snapshot(spark_delta, table), kinds=["chunk"]
    ).to_pylist()
    assert [r["file"] for r in live] == ["data/t/b.zpack"]


def test_a_commit_keeps_every_row_after_an_empty_chunk(spark_delta, table):
    """A concatenated table can carry zero-row chunks; Spark stops reading at the first."""
    w = uuid.uuid4().hex
    keys = [f"c/{i}/0/0" for i in range(5)]
    parts = [
        _rows(*(_chunk(k, w) for k in keys[:2])),
        _rows(),  # an empty part, as when one array contributes no rows
        _rows(*(_chunk(k, w) for k in keys[2:])),
    ]
    rows = pa.concat_tables(parts)
    assert [len(c) for c in rows.column("kind").chunks] == [2, 0, 3]
    M.commit(spark_delta, table, rows, write_id=w, meta={"mode": "create"})
    assert sorted(r["key"] for r in _live(spark_delta, table)) == keys


def test_stale_expectation_aborts_whole_commit(spark_delta, table):
    w1, w2, w3 = (uuid.uuid4().hex for _ in range(3))
    M.commit(spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={})
    before = M.snapshot(spark_delta, table).version
    stale = _rows(
        _chunk("c/0/0/1", w3), _chunk("c/0/0/0", w3, expected=w2)
    )  # w2 never wrote it
    with pytest.raises(ZarrWriteConflict):
        M.commit(spark_delta, table, stale, write_id=w3, meta={})
    assert M.snapshot(spark_delta, table).version == before  # nothing committed
    assert (
        M.read_rows(
            spark_delta, M.snapshot(spark_delta, table), kinds=["chunk"]
        ).num_rows
        == 1
    )


def test_idempotent_write_id(spark_delta, table):
    w1 = uuid.uuid4().hex
    v1 = M.commit(
        spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={}
    )
    assert (
        M.commit(spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={})
        == v1
    )
    assert M.find_commit_version(spark_delta, table, w1) == v1


def test_time_travel_snapshot(spark_delta, table):
    w1, w2 = uuid.uuid4().hex, uuid.uuid4().hex
    v1 = M.commit(
        spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={}
    )
    M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w2, expected=w1, file="data/t/b.zpack")),
        write_id=w2,
        meta={},
    )
    old = M.read_rows(
        spark_delta, M.snapshot(spark_delta, table, version_as_of=v1), kinds=["chunk"]
    ).to_pylist()
    assert old[0]["file"] == "data/t/a.zpack"


# --- ruling Z6: a stale commit names every conflicting key ------------------------


def test_stale_commit_lists_every_conflicting_key_and_the_winner(spark_delta, table):
    w1, w2, w3 = (uuid.uuid4().hex for _ in range(3))
    M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w1), _chunk("c/0/0/1", w1), _chunk("c/0/0/2", w1)),
        write_id=w1,
        meta={},
    )
    M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w2, expected=w1), _chunk("c/0/0/1", w2, expected=w1)),
        write_id=w2,
        meta={},
    )
    before = M.snapshot(spark_delta, table).version
    stale = _rows(  # w3 still believes w1 wrote all three; w2 replaced two of them
        _chunk("c/0/0/0", w3, expected=w1),
        _chunk("c/0/0/1", w3, expected=w1),
        _chunk("c/0/0/2", w3, expected=w1),
    )
    with pytest.raises(ZarrWriteConflict) as ei:
        M.commit(spark_delta, table, stale, write_id=w3, meta={})
    assert set(ei.value.keys) == {"t2m/c/0/0/0", "t2m/c/0/0/1"}
    assert ei.value.winner == w2
    assert "Fix:" in str(ei.value)
    assert M.snapshot(spark_delta, table).version == before


def test_changed_since_returns_rows_whose_live_write_id_differs(spark_delta, table):
    w1, w2 = uuid.uuid4().hex, uuid.uuid4().hex
    M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w1), _chunk("c/0/0/1", w1)),
        write_id=w1,
        meta={},
    )
    base = M.snapshot(spark_delta, table)
    M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w2, expected=w1)),
        write_id=w2,
        meta={},
    )
    ask = _rows(
        _chunk("c/0/0/0", w2, expected=w1),  # replaced by w2
        _chunk("c/0/0/1", w2, expected=w1),  # untouched
        _chunk("c/0/0/9", w2, expected=None),  # still absent: "must not exist" holds
        _chunk("c/0/0/8", w2, expected=w1),  # expected a row that does not exist
    )
    got = M.changed_since(spark_delta, table, base, ask).to_pylist()
    assert {(r["key"], r["write_id"]) for r in got} == {
        ("c/0/0/0", w2),
        ("c/0/0/8", None),
    }


# --- ruling Z7: classify by message text, whatever the exception class ------------

_CAS_OSS = "[USER_RAISED_EXCEPTION] GBX_ZARR_CAS chunk t2m c/0/0/1 SQLSTATE: P0001"
_CAS_COMMIT = (
    "[USER_RAISED_EXCEPTION] GBX_ZARR_CAS commit  " + "ab" * 16 + " SQLSTATE: P0001"
)
_CAS_GROUP = "[USER_RAISED_EXCEPTION] GBX_ZARR_CAS group   SQLSTATE: P0001"
_CAS_SERVERLESS = (
    _CAS_OSS
    + "\n\nJVM stacktrace:\norg.apache.spark.SparkRuntimeException: "
    + "[USER_RAISED_EXCEPTION]\n\tat com.databricks.photon.PhotonException"
)
# Errors that quote the MERGE (ANSI query context) hold the marker without being an abort.
_ECHOED_MERGE = (
    "[CAST_INVALID_INPUT] The value 'x' of the type \"STRING\" cannot be cast to "
    '"BIGINT" because it is malformed. SQLSTATE: 22018\n== SQL (line 11, position 3) ==\n'
    "  kind = raise_error(concat('GBX_ZARR_CAS ', s.kind, ' ', coalesce(s.array, ''), "
    "' ', coalesce(s.key, '')))\n"
)
_DBR19_APPEND = (
    "An error occurred while calling o312.sql.\n: "
    "io.delta.exceptions.ConcurrentAppendException: "
    "[DELTA_CONCURRENT_APPEND.ROW_LEVEL_CHANGES] Transaction conflict detected. "
    "A concurrent MERGE added data to table t committed at version 2. "
    "The concurrent operation modified the same rows that this transaction "
    "attempted to modify. Retry the operation."
)
_CONNECT_APPEND = (
    "(io.delta.exceptions.ConcurrentAppendException) "
    "[DELTA_CONCURRENT_APPEND.ROW_LEVEL_CHANGES] Transaction conflict detected."
)
_OSS_APPEND = (
    "An error occurred while calling o99.sql.\n: "
    "io.delta.exceptions.ConcurrentAppendException: [DELTA_CONCURRENT_APPEND] "
    "ConcurrentAppendException: Files were added to the root of the table by a "
    "concurrent update. Please try the operation again."
)


@pytest.mark.parametrize(
    "text, expected",
    [
        (_CAS_OSS, "cas"),
        (_CAS_COMMIT, "cas"),
        (_CAS_GROUP, "cas"),
        (_CAS_SERVERLESS, "cas"),
        (_CAS_OSS + " ConcurrentAppendException", "cas"),  # the CAS marker wins
        (_DBR19_APPEND, "concurrent"),
        (_CONNECT_APPEND, "concurrent"),
        (_OSS_APPEND, "concurrent"),
        ("[DELTA_CONCURRENT_DELETE.READ] a file was deleted", "concurrent"),
        ("[DELTA_CONCURRENT_TRANSACTION] a concurrent transaction", "concurrent"),
        ("[DELTA_CONCURRENT_APPEND.ROW_LEVEL_CHANGES]", "concurrent"),
        ("... DELTA_ROW_LEVEL_CHANGES ROW_LEVEL_CHANGES ...", "concurrent"),
        ("java.util.ConcurrentModificationException: boom", "concurrent"),
        ("[DELTA_METADATA_CHANGED] The metadata of the table changed", "concurrent"),
        ("io.delta.exceptions.MetadataChangedException: changed", "concurrent"),
        ("[TABLE_OR_VIEW_NOT_FOUND] The table cannot be found", None),
        ("[DELTA_PROTOCOL_CHANGED] protocol", None),
        (_ECHOED_MERGE, None),
        ("GBX_ZARR_CAS bogus t2m c/0/0/1", None),
        ("", None),
    ],
)
def test_classify_commit_error_by_text(text, expected):
    assert M.classify_commit_error(text) == expected


@pytest.mark.parametrize(
    "text, parsed",
    [
        (_CAS_OSS, ("chunk", "t2m", "c/0/0/1")),
        (_CAS_COMMIT, ("commit", "", "ab" * 16)),
        (_CAS_GROUP, ("group", "", "")),
        (_CAS_SERVERLESS, ("chunk", "t2m", "c/0/0/1")),
        (_ECHOED_MERGE, None),
        (_DBR19_APPEND, None),
    ],
)
def test_parse_cas_failure_names_the_first_failing_key(text, parsed):
    assert M.parse_cas_failure(text) == parsed


class _FailMerge:
    """A session stand-in whose MERGE raises; everything else passes through."""

    def __init__(self, spark, exc):
        self._spark, self._exc = spark, exc

    def __getattr__(self, name):
        return getattr(self._spark, name)

    def sql(self, query, *args, **kwargs):
        if query.lstrip().upper().startswith("MERGE"):
            raise self._exc
        return self._spark.sql(query, *args, **kwargs)


def test_commit_maps_concurrent_errors_to_retryable_and_passes_others(
    spark_delta, table
):
    w1 = uuid.uuid4().hex
    rows = _rows(_chunk("c/0/0/0", w1))
    before = M.snapshot(spark_delta, table).version
    busy = RuntimeError(_DBR19_APPEND)  # classic: a Py4J-style error, any class will do
    with pytest.raises(M.ConcurrentCommit):
        M.commit(_FailMerge(spark_delta, busy), table, rows, write_id=w1, meta={})
    other = RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] nope")
    with pytest.raises(RuntimeError) as ei:
        M.commit(_FailMerge(spark_delta, other), table, rows, write_id=w1, meta={})
    assert ei.value is other
    assert M.snapshot(spark_delta, table).version == before


# --- ruling Z8: versions older than the vacuum horizon are gone -------------------


def _tag_row(spark, table, tag, write_id="f" * 32):
    """What a tag leaves behind: a copy of a live row carrying ``tag=name`` (a later
    task owns the real write)."""
    spark.sql(
        f"INSERT INTO {table} (kind, array, key, shard_key, write_id, tag) "
        f"VALUES ('chunk', 't2m', 'c/0/0/0', 'c/0/0/0', '{write_id}', '{tag}')"
    )


def test_snapshot_refuses_versions_older_than_the_vacuum_horizon(spark_delta, table):
    w1, w2, w3 = (uuid.uuid4().hex for _ in range(3))
    v1 = M.commit(
        spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={}
    )
    v2 = M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w2, expected=w1)),
        write_id=w2,
        meta={},
    )
    ts_v1 = (
        spark_delta.sql(f"DESCRIBE HISTORY {table}")
        .where(F.col("version") == v1)
        .select(F.col("timestamp").cast("string"))
        .first()[0]
    )
    assert M.snapshot(spark_delta, table, version_as_of=v1).version == v1  # no horizon

    time.sleep(1.1)  # the horizon lies strictly after v1 and v2 and before what follows
    horizon = datetime.now(timezone.utc).isoformat()
    spark_delta.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ('geobrix.zarr.vacuum_horizon' = '{horizon}')"
    )
    v_prop = M.snapshot(spark_delta, table).version  # the ALTER's own version
    v4 = M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w3, expected=w2)),
        write_id=w3,
        meta={},
    )
    _tag_row(spark_delta, table, "keep")
    v_tag = M.snapshot(spark_delta, table).version

    # v1 was replaced before the horizon: gone. v2 was the live version AT the horizon,
    # so a vacuum keeps what it needs: readable. Everything after is readable.
    with pytest.raises(ZarrVersionUnavailable) as ei:
        M.snapshot(spark_delta, table, version_as_of=v1)
    assert "gbx_zarr_tag" in ei.value.fix
    with pytest.raises(ZarrVersionUnavailable):
        M.snapshot(spark_delta, table, timestamp_as_of=ts_v1)
    for kept in (v2, v_prop, v4, v_tag):
        assert M.snapshot(spark_delta, table, version_as_of=kept).version == kept
    # a tag is read at the latest version and keeps its rows whatever the horizon says
    tagged = M.snapshot(spark_delta, table, version_as_of="tag:keep")
    assert tagged == M.Snapshot(table=table, version=v_tag, tag="keep")

    # a horizon in the future: only the version live at it (the latest) is readable
    far = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    spark_delta.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ('geobrix.zarr.vacuum_horizon' = '{far}')"
    )
    latest = M.snapshot(spark_delta, table).version
    assert M.snapshot(spark_delta, table, version_as_of=latest).version == latest
    for gone in (v2, v4, v_tag):
        with pytest.raises(ZarrVersionUnavailable):
            M.snapshot(spark_delta, table, version_as_of=gone)


def test_snapshot_refuses_versions_older_than_the_retain_window(spark_delta):
    """Delta's own vacuum may have removed the files of a version that left the retain
    window, so ``snapshot`` refuses it even when no GeoBrix vacuum has run."""
    name = f"zm_{uuid.uuid4().hex[:8]}"
    M.create_manifest(spark_delta, name, dataclasses.replace(PROPS, retain="1 second"))
    try:
        ws = [uuid.uuid4().hex for _ in range(3)]
        vs = [
            M.commit(
                spark_delta,
                name,
                _rows(_chunk("c/0/0/0", w, expected=prev)),
                write_id=w,
                meta={},
            )
            for w, prev in zip(ws, [None, *ws])
        ]
        time.sleep(1.1)  # every commit is now older than the 1 second window
        with pytest.raises(ZarrVersionUnavailable):
            M.snapshot(spark_delta, name, version_as_of=vs[0])
        with pytest.raises(ZarrVersionUnavailable):
            M.snapshot(spark_delta, name, version_as_of=vs[1])
        # the version live when the window opened, and the latest, stay readable
        assert M.snapshot(spark_delta, name, version_as_of=vs[2]).version == vs[2]
        assert M.snapshot(spark_delta, name).version >= vs[2]
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {name}")


def test_snapshot_of_a_missing_tag_names_the_tag(spark_delta, table):
    w1 = uuid.uuid4().hex
    M.commit(spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={})
    with pytest.raises(ZarrVersionUnavailable) as ei:
        M.snapshot(spark_delta, table, version_as_of="tag:nope")
    assert "nope" in str(ei.value) and "Fix:" in str(ei.value)
    _tag_row(spark_delta, table, "there")
    assert M.snapshot(spark_delta, table, version_as_of="tag:there").tag == "there"


def test_snapshot_rejects_a_malformed_horizon(spark_delta, table):
    w1 = uuid.uuid4().hex
    v1 = M.commit(
        spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={}
    )
    spark_delta.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ('geobrix.zarr.vacuum_horizon' = 'last tuesday')"
    )
    with pytest.raises(ZarrError) as ei:
        M.snapshot(spark_delta, table, version_as_of=v1)
    assert "vacuum_horizon" in str(ei.value) and "Fix:" in str(ei.value)


# --- snapshot, version lookup and reads -------------------------------------------


def test_snapshot_timestamp_as_of_is_latest_version_at_or_before(spark_delta, table):
    w1, w2 = uuid.uuid4().hex, uuid.uuid4().hex
    v1 = M.commit(
        spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={}
    )
    time.sleep(1.1)
    v2 = M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w2, expected=w1)),
        write_id=w2,
        meta={},
    )
    hist = {
        r["version"]: r["ts"]
        for r in spark_delta.sql(f"DESCRIBE HISTORY {table}")
        .select("version", F.col("timestamp").cast("string").alias("ts"))
        .collect()
    }
    assert M.snapshot(spark_delta, table, timestamp_as_of=hist[v1]).version == v1
    assert M.snapshot(spark_delta, table, timestamp_as_of=hist[v2]).version == v2
    midway = spark_delta.sql(
        f"SELECT cast(cast('{hist[v1]}' as timestamp) + interval 500 milliseconds as string)"
    ).first()[0]
    assert M.snapshot(spark_delta, table, timestamp_as_of=midway).version == v1
    with pytest.raises(ZarrVersionUnavailable):
        M.snapshot(spark_delta, table, timestamp_as_of="2001-01-01 00:00:00")


def test_snapshot_unknown_version_is_unavailable_with_a_fix(spark_delta, table):
    with pytest.raises(ZarrVersionUnavailable) as ei:
        M.snapshot(spark_delta, table, version_as_of=999)
    assert "gbx_zarr_tag" in ei.value.fix


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(version_as_of=1, timestamp_as_of="2020-01-01"),
        dict(version_as_of="v3"),
        dict(version_as_of="tag:"),
        dict(version_as_of="tag:it's"),
        dict(version_as_of=True),
        dict(version_as_of=1.5),
    ],
)
def test_snapshot_rejects_malformed_arguments(kwargs):
    with pytest.raises(ValueError):
        M.snapshot(None, "t", **kwargs)


def test_read_rows_filters_and_tag_snapshots(spark_delta, table):
    w1 = uuid.uuid4().hex
    other = _chunk("c/0/0/1", w1)
    other.update(array="u10", shard_key="c/1/0/0")
    arr = dict(kind="array", array="t2m", key=None, write_id=w1, op="upsert", meta="{}")
    M.commit(
        spark_delta,
        table,
        _rows(arr, _chunk("c/0/0/0", w1), other),
        write_id=w1,
        meta={},
    )
    # a tag copies live rows with tag=name (a later task owns that write)
    spark_delta.sql(
        f"INSERT INTO {table} (kind, array, key, shard_key, write_id, tag) "
        f"VALUES ('chunk', 't2m', 'c/0/0/0', 'c/0/0/0', 'f' , 'v1')"
    )
    snap = M.snapshot(spark_delta, table)
    assert len(_keys(M.read_rows(spark_delta, snap))) == 4  # arr, 2 chunks, commit row
    assert _keys(M.read_rows(spark_delta, snap, kinds=["array"])) == [
        ("array", "t2m", None)
    ]
    assert _keys(M.read_rows(spark_delta, snap, arrays=["u10"])) == [
        ("chunk", "u10", "c/0/0/1")
    ]
    assert _keys(
        M.read_rows(spark_delta, snap, kinds=["chunk"], shard_keys=["c/1/0/0"])
    ) == [("chunk", "u10", "c/0/0/1")]
    tagged = M.snapshot(spark_delta, table, version_as_of="tag:v1")
    assert tagged.tag == "v1" and tagged.version == snap.version
    assert _keys(M.read_rows(spark_delta, tagged)) == [("chunk", "t2m", "c/0/0/0")]
    assert M.read_rows(spark_delta, snap).schema.names == list(M.MANIFEST_COLUMNS)
    with pytest.raises(ValueError):
        M.read_rows(spark_delta, snap, kinds=["bogus"])


def test_read_rows_filters_by_key_for_arrays_without_shards(spark_delta, table):
    """Chunk rows of an unsharded array have no shard_key, so a read prunes them by key."""
    w1 = uuid.uuid4().hex
    rows = []
    for key in ("c/0/0/0", "c/0/0/1", "c/1/0/0"):
        row = _chunk(key, w1)
        row.update(shard_key=None)
        rows.append(row)
    M.commit(spark_delta, table, _rows(*rows), write_id=w1, meta={})
    snap = M.snapshot(spark_delta, table)
    got = M.read_rows(spark_delta, snap, kinds="chunk", keys=["c/0/0/1", "c/1/0/0"])
    assert _keys(got) == [("chunk", "t2m", "c/0/0/1"), ("chunk", "t2m", "c/1/0/0")]
    assert M.read_rows(spark_delta, snap, kinds="chunk", keys=[]).num_rows == 0
    assert M.read_rows(spark_delta, snap, kinds="chunk", keys="c/0/0/0").num_rows == 1


def test_commit_delete_removes_the_row_under_cas(spark_delta, table):
    w1, w2, w3 = (uuid.uuid4().hex for _ in range(3))
    M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w1), _chunk("c/0/0/1", w1)),
        write_id=w1,
        meta={},
    )
    M.commit(spark_delta, table, _rows(_delete("c/0/0/0", w1)), write_id=w2, meta={})
    assert [r["key"] for r in _live(spark_delta, table)] == ["c/0/0/1"]
    with pytest.raises(ZarrWriteConflict):  # already gone: the expectation is stale
        M.commit(
            spark_delta, table, _rows(_delete("c/0/0/0", w1)), write_id=w3, meta={}
        )


def test_commit_row_records_meta_and_find_commit_version_respects_since(
    spark_delta, table
):
    w1, w2 = uuid.uuid4().hex, uuid.uuid4().hex
    v1 = M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w1)),
        write_id=w1,
        meta={"mode": "create"},
    )
    v2 = M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w2, expected=w1)),
        write_id=w2,
        meta={"mode": "append"},
    )
    commits = M.read_rows(spark_delta, M.snapshot(spark_delta, table), kinds=["commit"])
    assert {r["key"]: r["meta"] for r in commits.to_pylist()} == {
        w1: '{"mode": "create"}',
        w2: '{"mode": "append"}',
    }
    assert M.find_commit_version(spark_delta, table, w2, since=v1) == v2
    assert M.find_commit_version(spark_delta, table, w1, since=v2) is None
    assert M.find_commit_version(spark_delta, table, uuid.uuid4().hex) is None


def _commit_two(spark, table):
    w1, w2 = uuid.uuid4().hex, uuid.uuid4().hex
    v1 = M.commit(spark, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={})
    v2 = M.commit(
        spark, table, _rows(_chunk("c/0/0/0", w2, expected=w1)), write_id=w2, meta={}
    )
    return (w1, v1), (w2, v2)


def test_commit_lookup_survives_delta_vacuuming_the_change_feed(spark_delta, table):
    """Predictive optimization vacuums change files long before the log expires. The
    commit row itself carries its Delta version (row tracking), so neither
    ``find_commit_version`` nor a default ``commit`` may depend on the change feed."""
    (w1, v1), (w2, v2) = _commit_two(spark_delta, table)
    spark_delta.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES "
        "('delta.deletedFileRetentionDuration' = 'interval 0 hours')"
    )
    spark_delta.sql(f"VACUUM {table} RETAIN 0 HOURS")
    with pytest.raises(Exception):  # the precondition: the change files are really gone
        spark_delta.sql(f"SELECT * FROM table_changes('{table}', 0)").collect()

    assert M.find_commit_version(spark_delta, table, w1) == v1
    assert M.find_commit_version(spark_delta, table, w2) == v2
    assert M.find_commit_version(spark_delta, table, uuid.uuid4().hex) is None
    w3 = uuid.uuid4().hex
    v3 = M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w3, expected=w2)),
        write_id=w3,
        meta={},
    )
    assert v3 > v2
    assert M.find_commit_version(spark_delta, table, w3) == v3
    replay = _rows(_chunk("c/0/0/0", w1))
    assert M.commit(spark_delta, table, replay, write_id=w1, meta={}) == v1


def test_commit_versions_survive_file_rewrites(spark_delta, table):
    (w1, v1), (w2, v2) = _commit_two(spark_delta, table)
    rewrote = spark_delta.sql(f"OPTIMIZE {table}").first()["metrics"]["numFilesRemoved"]
    assert rewrote > 0  # the commits' files really were compacted into new ones
    spark_delta.sql(f"DELETE FROM {table} WHERE kind = 'chunk'")
    spark_delta.sql(f"REORG TABLE {table} APPLY (PURGE)")
    assert M.find_commit_version(spark_delta, table, w1) == v1
    assert M.find_commit_version(spark_delta, table, w2) == v2


def test_a_replay_that_slips_past_the_precheck_returns_its_version(
    spark_delta, table, monkeypatch
):
    """Two twins of one write_id: the loser passes the pre-check before the winner's
    commit lands, then hits the CAS guard. It must return the winner's version, not
    raise a conflict against itself."""
    w1 = uuid.uuid4().hex
    rows = _rows(_chunk("c/0/0/0", w1))
    v1 = M.commit(spark_delta, table, rows, write_id=w1, meta={})
    real, calls = M.find_commit_version, []

    def blind_once(*args, **kwargs):
        calls.append(1)
        return None if len(calls) == 1 else real(*args, **kwargs)

    monkeypatch.setattr(M, "find_commit_version", blind_once)
    assert M.commit(spark_delta, table, rows, write_id=w1, meta={}) == v1
    assert len(calls) == 2  # the blind pre-check, then the lookup in the CAS branch
    assert M.snapshot(spark_delta, table).version == v1


@pytest.mark.parametrize("lookup", ["raises", "empty"])
def test_a_conflict_survives_a_failed_diagnosis(
    spark_delta, table, monkeypatch, lookup
):
    """If listing every conflicting key fails, the write still ends as a
    ZarrWriteConflict that names the key the abort reported."""
    w1, w2, w3 = (uuid.uuid4().hex for _ in range(3))
    M.commit(spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={})

    def broken(*args, **kwargs):
        if lookup == "raises":
            raise RuntimeError("diagnosis unavailable")
        return pa.table(
            {c: pa.array([], pa.string()) for c in ("kind", "array", "key")}
        )

    monkeypatch.setattr(M, "changed_since", broken)
    stale = _rows(_chunk("c/0/0/0", w3, expected=w2))  # w2 never wrote it
    with pytest.raises(ZarrWriteConflict) as ei:
        M.commit(spark_delta, table, stale, write_id=w3, meta={})
    assert ei.value.keys == ("t2m/c/0/0/0",)
    assert ei.value.winner is None and "Fix:" in str(ei.value)


def test_a_failed_lookup_in_the_cas_branch_still_ends_as_a_conflict(
    spark_delta, table, monkeypatch
):
    """After a CAS abort ``commit`` looks for a twin of its own write_id. If that lookup
    fails, the user still gets the ZarrWriteConflict, not the lookup's error."""
    w1, w2, w3 = (uuid.uuid4().hex for _ in range(3))
    M.commit(spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={})
    real, calls = M.find_commit_version, []

    def fails_after_the_precheck(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            return real(*args, **kwargs)  # the pre-check: no such commit yet
        raise RuntimeError("lookup unavailable")

    monkeypatch.setattr(M, "find_commit_version", fails_after_the_precheck)
    stale = _rows(_chunk("c/0/0/0", w3, expected=w2))  # w2 never wrote it
    with pytest.raises(ZarrWriteConflict) as ei:
        M.commit(spark_delta, table, stale, write_id=w3, meta={})
    assert len(calls) == 2  # the pre-check, then the lookup that failed
    assert ei.value.keys == ("t2m/c/0/0/0",) and "Fix:" in str(ei.value)


def test_row_tracking_turned_off_is_a_typed_error_from_the_precheck(spark_delta, table):
    """The commit version comes from row tracking. With it off the lookup used to fail
    with a raw FIELD_NOT_FOUND from Spark; the pre-check now says what to fix."""
    spark_delta.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.enableRowTracking' = 'false')"
    )
    w1 = uuid.uuid4().hex
    with pytest.raises(ZarrError) as ei:
        M.find_commit_version(spark_delta, table, w1)
    assert "enableRowTracking" in ei.value.fix and "Fix:" in str(ei.value)
    with pytest.raises(ZarrError) as ei:
        M.commit(spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={})
    assert "enableRowTracking" in ei.value.fix
    assert _live(spark_delta, table) == []  # the pre-check refused: nothing was merged


def test_a_missing_table_is_not_blamed_on_row_tracking(spark_delta):
    """Spark's error for a missing table prints the unresolved plan, which projects
    ``_metadata.row_commit_version``; only a FIELD_NOT_FOUND for that field means row
    tracking is off."""
    with pytest.raises(Exception) as ei:
        M.find_commit_version(
            spark_delta, f"zm_{uuid.uuid4().hex[:8]}", uuid.uuid4().hex
        )
    assert not isinstance(ei.value, ZarrError)
    assert "enableRowTracking" not in str(ei.value)


def _count_metadata_queries(monkeypatch, spark):
    """Count the DataFrame actions (``collect``, which ``first`` and ``take`` end in)."""
    from pyspark.sql.classic.dataframe import DataFrame as ClassicDataFrame

    actions = []
    real = ClassicDataFrame.collect

    def counting(self):
        actions.append(1)
        return real(self)

    monkeypatch.setattr(ClassicDataFrame, "collect", counting)
    return actions


@pytest.mark.parametrize("how", ["version", "timestamp"])
def test_snapshot_of_a_past_version_reads_the_history_once(
    spark_delta, table, monkeypatch, how
):
    w1, w2 = uuid.uuid4().hex, uuid.uuid4().hex
    v1 = M.commit(
        spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={}
    )
    M.commit(
        spark_delta,
        table,
        _rows(_chunk("c/0/0/0", w2, expected=w1)),
        write_id=w2,
        meta={},
    )
    kwargs = (
        {"version_as_of": v1}
        if how == "version"
        else {"timestamp_as_of": datetime.now(timezone.utc)}
    )
    actions = _count_metadata_queries(monkeypatch, spark_delta)
    snap = M.snapshot(spark_delta, table, **kwargs)
    # one for the table history, one for SHOW TBLPROPERTIES (the timestamp form adds the
    # one that parses its argument)
    assert len(actions) <= (2 if how == "version" else 3), len(actions)
    assert snap.version == (
        v1 if how == "version" else M.snapshot(spark_delta, table).version
    )


def test_snapshot_rejects_a_malformed_retain_property(spark_delta, table):
    w1 = uuid.uuid4().hex
    v1 = M.commit(
        spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={}
    )
    spark_delta.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ('geobrix.zarr.retain' = 'a while')"
    )
    with pytest.raises(ZarrError) as ei:
        M.snapshot(spark_delta, table, version_as_of=v1)
    assert "geobrix.zarr.retain" in str(ei.value) and "Fix:" in str(ei.value)
    assert "7 days" in ei.value.fix  # the fix shows the form


def test_conflict_rows_ride_on_the_error(spark_delta, table):
    w1, w2 = uuid.uuid4().hex, uuid.uuid4().hex
    M.commit(spark_delta, table, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={})
    with pytest.raises(ZarrWriteConflict) as ei:
        M.commit(
            spark_delta,
            table,
            _rows(_chunk("c/0/0/0", w2, expected=uuid.uuid4().hex)),
            write_id=w2,
            meta={},
        )
    (row,) = ei.value.conflicts
    assert (row["kind"], row["array"], row["key"], row["write_id"]) == (
        "chunk",
        "t2m",
        "c/0/0/0",
        w1,
    )


def test_a_user_named_table_is_quoted_everywhere(spark_delta):
    name = f"spark_catalog.default.`zm_{uuid.uuid4().hex[:8]}`"  # Delta bars odd characters
    M.create_manifest(spark_delta, name, PROPS)
    try:
        w1 = uuid.uuid4().hex
        v1 = M.commit(
            spark_delta, name, _rows(_chunk("c/0/0/0", w1)), write_id=w1, meta={}
        )
        assert M.find_commit_version(spark_delta, name, w1) == v1
        assert M.read_props(spark_delta, name) == PROPS
        assert len(_live(spark_delta, name)) == 1
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {name}")


def test_registered_props_round_trip(spark_delta):
    props = M.StoreProps(
        kind="registered",
        root="/Volumes/c/s/v/it's\\here",
        overlay_root="/Volumes/c/s/v/overlay",
        writable=True,
        file_mode="external",
        retain="36 hours",
    )
    name = f"zm_{uuid.uuid4().hex[:8]}"
    M.create_manifest(spark_delta, name, props)
    try:
        assert M.read_props(spark_delta, name) == props
        tp = {
            r["key"]: r["value"]
            for r in spark_delta.sql(f"SHOW TBLPROPERTIES {name}").collect()
        }
        assert tp["delta.deletedFileRetentionDuration"] == "interval 2 days"
        assert tp["delta.logRetentionDuration"] == "interval 30 days"
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {name}")


def test_read_props_refuses_a_table_that_is_not_a_manifest(spark_delta):
    name = f"zm_plain_{uuid.uuid4().hex[:8]}"
    spark_delta.sql(f"CREATE TABLE {name} (a INT) USING DELTA")
    try:
        with pytest.raises(ZarrOwnershipError) as ei:
            M.read_props(spark_delta, name)
        assert "Fix:" in str(ei.value)
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {name}")


# --- inputs that never reach SQL --------------------------------------------------


@pytest.mark.parametrize(
    "raw, quoted",
    [
        ("t", "`t`"),
        ("a.b.c", "`a`.`b`.`c`"),
        ("`a.b`.c", "`a.b`.`c`"),
        ("`we``ird`", "`we``ird`"),
        ("`x``y`.z", "`x``y`.`z`"),
    ],
)
def test_q_quotes_each_dotted_part(raw, quoted):
    assert M._q(raw) == quoted


@pytest.mark.parametrize("bad", ["", "a..b", ".a", "a.", "a.b.c.d", "`a", "a`b`c"])
def test_q_rejects_malformed_names(bad):
    with pytest.raises(ValueError):
        M._q(bad)


@pytest.mark.parametrize(
    "write_id",
    ["", "abc", "A" * 32, "g" * 32, "a" * 31, "a" * 33, "a" * 32 + "'", None, 12],
)
def test_write_ids_are_32_lowercase_hex_before_any_sql(write_id):
    rows = _rows(_chunk("c/0/0/0", "a" * 32))
    with pytest.raises(ValueError):
        M.find_commit_version(None, "t", write_id)
    with pytest.raises(ValueError):
        M.commit(None, "t", rows, write_id=write_id, meta={})


def test_commit_rejects_ill_formed_rows_before_any_sql():
    w = "a" * 32
    good = _chunk("c/0/0/0", w)
    with pytest.raises(ValueError, match="duplicate"):
        M.commit(None, "t", _rows(good, good), write_id=w, meta={})
    meta_row = {**good, "kind": "group", "array": None, "key": None}
    with pytest.raises(ValueError, match="duplicate"):  # null keys are keys too
        M.commit(None, "t", _rows(meta_row, meta_row), write_id=w, meta={})
    with pytest.raises(ValueError, match="tag"):
        M.commit(None, "t", _rows({**good, "tag": "v1"}), write_id=w, meta={})
    with pytest.raises(ValueError, match="write_id"):
        M.commit(None, "t", _rows(_chunk("c/0/0/0", "b" * 32)), write_id=w, meta={})
    with pytest.raises(ValueError, match="op"):
        M.commit(None, "t", _rows({**good, "op": "merge"}), write_id=w, meta={})
    with pytest.raises(ValueError, match="commit"):
        M.commit(None, "t", _rows({**good, "kind": "commit"}), write_id=w, meta={})
    with pytest.raises(ValueError, match="kind"):
        M.commit(None, "t", _rows({**good, "kind": "bogus"}), write_id=w, meta={})
    with pytest.raises(ValueError, match="missing"):
        M.commit(None, "t", _rows(good).drop(["op"]), write_id=w, meta={})
    with pytest.raises(ValueError, match="op"):  # a null op is not an op
        M.commit(None, "t", _rows({**good, "op": None}), write_id=w, meta={})
    with pytest.raises(ValueError, match="kind"):  # nor is a null kind
        M.commit(None, "t", _rows({**good, "kind": None}), write_id=w, meta={})


def test_commit_validation_handles_a_large_commit():
    """Validation is columnar: a commit can carry millions of chunk rows."""
    n, w = 300_000, "a" * 32
    rows = pa.table(
        {
            "kind": pa.array(["chunk"] * n),
            "array": pa.array(["t2m"] * n),
            "key": pa.array([f"c/0/0/{i}" for i in range(n)]),
            "write_id": pa.array([w] * n),
            "op": pa.array(["upsert"] * n),
            "expected_write_id": pa.array([None] * n, pa.string()),
        }
    )
    out = M._commit_source(rows, w, {"mode": "bulk"})
    assert out.num_rows == n + 1 and out.schema.names == list(M._COMMIT_COLUMNS)
    dup = pa.concat_tables([rows, rows.slice(n - 1, 1)])
    with pytest.raises(ValueError, match="duplicate"):
        M._commit_source(dup, w, {})


@pytest.mark.parametrize(
    "bad",
    [
        dict(kind="shared"),
        dict(file_mode="auto"),
        dict(retain="a while"),
        dict(kind="managed", overlay_root="/x"),
        dict(kind="registered", writable=False, overlay_root="/x"),
    ],
)
def test_create_manifest_validates_props_before_any_sql(bad):
    with pytest.raises(ValueError):
        M.create_manifest(None, "t", dataclasses.replace(PROPS, **bad))


@pytest.mark.parametrize(
    "retain, seconds",
    [
        ("7 days", 604800),
        ("1 day", 86400),
        ("36 hours", 129600),
        ("1 hour", 3600),
        ("2 weeks", 1209600),
        ("90 minutes", 5400),
        ("1.5 days", 129600),
    ],
)
def test_retain_parses_to_seconds(retain, seconds):
    assert M.retain_seconds(retain) == seconds


@pytest.mark.parametrize(
    "retain, days",
    [("7 days", 7), ("36 hours", 2), ("1 hour", 1), ("8 days", 8), ("1 week", 7)],
)
def test_retain_days_round_up_with_a_floor_of_one(retain, days):
    assert M.retain_days(retain) == days
