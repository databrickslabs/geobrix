"""File modes (spec 4.3): which mode a call gets, the DDL and MERGE each mode emits, and how a
FILE value maps back to the path string the rest of the store uses.

FILE does not exist in OSS Spark, so these use a fake probe and assert the SQL text. The
one end-to-end test emulates the FILE type with a one-field struct and a ``try_to_file``
function, to run the manifest's own commit and read paths; what FILE itself does is gate
G4, on a FILE-enabled cluster.
"""

import os
import uuid

import pyarrow as pa
import pytest

from databricks.labs.gbx.zarr import _filemode as FM
from databricks.labs.gbx.zarr import _maint as MT
from databricks.labs.gbx.zarr import _manifest as M
from databricks.labs.gbx.zarr._access import FuseRangeReader
from databricks.labs.gbx.zarr._register import gbx_zarr_register
from databricks.labs.gbx.zarr._shard import Region
from databricks.labs.gbx.zarr.errors import ZarrStoreDrift, ZarrUnsupported

NOT_VERIFIED = "were not verified here"


@pytest.fixture
def file_support(monkeypatch):
    """A runtime with FILE, and the verified modes set to ``modes`` (none: the gate)."""

    def make(supported=True, modes=("external", "managed")):
        monkeypatch.setattr(FM, "file_supported", lambda spark: supported)
        monkeypatch.setattr(FM, "VERIFIED_FILE_MODES", frozenset(modes))

    return make


class Recorder:
    """A session that only records the SQL it is given."""

    def __init__(self):
        self.sql_text = []

    def sql(self, text):
        self.sql_text.append(text)


def _props(mode, **kw):
    base = dict(
        kind="managed",
        root="/Volumes/c/s/v/store",
        overlay_root=None,
        writable=True,
        file_mode=mode,
        retain="7 days",
    )
    return M.StoreProps(**{**base, **kw})


# --- which mode a call gets ----------------------------------------------------------------


@pytest.mark.parametrize(
    "supported, modes",
    [
        (False, ("external", "managed")),  # no FILE on this runtime
        (True, ()),  # FILE, but G4 has not verified a mode: the shipped state
    ],
)
def test_auto_resolves_to_fuse_without_file_support(file_support, supported, modes):
    file_support(supported, modes)
    for filespace in (True, False):
        assert FM.resolve(None, "auto", has_filespace=filespace) == "fuse"
    assert FM.resolve(None, "fuse", has_filespace=True) == "fuse"


def test_the_g4_gate_ships_closed(monkeypatch):
    """No FILE mode is verified yet: a passing G4 adds it to VERIFIED_FILE_MODES, and this
    test with it."""
    monkeypatch.setattr(FM, "file_supported", lambda spark: True)
    assert FM.VERIFIED_FILE_MODES == frozenset()
    assert FM.resolve(None, "auto", has_filespace=True) == "fuse"
    with pytest.raises(ZarrUnsupported, match=NOT_VERIFIED):
        FM.resolve(None, "external", has_filespace=True)


@pytest.mark.parametrize("mode", ["external", "managed"])
@pytest.mark.parametrize(
    "supported, modes, text",
    [
        (True, (), NOT_VERIFIED),  # the gate: shipped state, even on a FILE runtime
        (False, ("external", "managed"), "does not support FILE"),
    ],
)
def test_explicit_managed_raises_without_file_support(
    file_support, mode, supported, modes, text
):
    file_support(supported, modes)
    with pytest.raises(ZarrUnsupported, match=text) as e:
        FM.resolve(None, mode, has_filespace=True)
    assert 'use file_mode="fuse"' in e.value.fix and "Fix:" in str(e.value)


def test_a_verified_runtime_picks_the_mode_by_its_filespace(file_support):
    file_support()
    assert FM.resolve(None, "auto", has_filespace=True) == "managed"
    assert FM.resolve(None, "auto", has_filespace=False) == "external"
    assert FM.resolve(None, "managed", has_filespace=False) == "managed"
    file_support(True, ("external",))  # managed alone is not verified: auto skips it
    assert FM.resolve(None, "auto", has_filespace=True) == "external"
    with pytest.raises(ValueError, match=r"file_mode.*Fix:"):
        FM.resolve(None, "s3", has_filespace=False)


def test_registered_base_never_managed(file_support):
    file_support()  # even where managed is verified and supported
    with pytest.raises(ZarrUnsupported, match="duplicated") as e:
        gbx_zarr_register(None, "/Volumes/c/s/v/base", "t", file_mode="managed")
    assert "external" in e.value.fix
    # auto with an overlay (the filespace a registered store could offer) is external
    assert FM.resolve(None, "auto", has_filespace=True, registered=True) == "external"
    with pytest.raises(ValueError, match="registered"):
        M.create_manifest(None, "t", _props("managed", kind="registered", root="/base"))


# --- the DDL and the commit each mode emits ------------------------------------------------


@pytest.mark.parametrize(
    "mode, column, filespace",
    [
        ("fuse", "file STRING,", None),
        ("external", "file FILE EXTERNAL,", None),
        ("managed", "file FILE MANAGED,", "/Volumes/c/s/v/store"),
    ],
)
def test_ddl_uses_file_managed_column_when_supported(
    file_support, mode, column, filespace
):
    file_support()
    spark = Recorder()
    resolved = FM.resolve(spark, mode, has_filespace=True)
    M.create_manifest(spark, "t", _props(resolved))
    (ddl,) = spark.sql_text
    assert column in ddl
    assert ("databricks.filespace-preview" in ddl) == bool(filespace)
    if filespace:
        assert f"'databricks.filespace-preview' = '{filespace}'" in ddl
    assert f"'geobrix.zarr.file_mode' = '{mode}'" in ddl


def test_a_managed_filespace_is_the_overlay_root_when_there_is_one():
    assert FM.table_properties("managed", "/data", "/overlay") == {
        "databricks.filespace-preview": "/overlay"
    }
    assert FM.table_properties("external", "/data") == {}


def test_the_merge_stores_the_files_the_way_the_mode_keeps_them():
    fuse = M._merge_sql("t", "v", FM.FileLayout("fuse", "/own"))
    assert "file = s.file," in fuse and "try_to_file" not in fuse
    assert "USING v AS s" in fuse

    layout = FM.FileLayout("external", gbx_root="/own", base_root="/base/")
    sql = M._merge_sql("t", "v", layout)
    assert "file = CASE WHEN s.file_path IS NULL THEN NULL ELSE coalesce(" in sql
    assert "try_to_file(s.file_path)" in sql
    # relative paths join the root they lie under; an absolute path is kept
    assert "startswith(v.file, '/') THEN v.file" in sql
    assert "v.origin = 'external' THEN concat('/base/', v.file)" in sql
    assert "ELSE concat('/own/', v.file)" in sql
    # a path that is not a file aborts the MERGE: try_to_file would give NULL, a tombstone
    assert "raise_error(concat('GBX_ZARR_FILE_MISSING '" in sql

    with pytest.raises(ZarrUnsupported, match="does not write") as e:
        M._merge_sql("t", "v", FM.FileLayout("managed", "/own"))
    assert "Fix:" in str(e.value)


# --- FILE value -> path ---------------------------------------------------------------------


def test_path_of_strips_dbfs_prefix():
    uri = "dbfs:/Volumes/c/s/fs/0f1e2d"
    assert FM.path_of({"uri": uri, "size": 4}) == "/Volumes/c/s/fs/0f1e2d"
    assert FM.path_of(uri) == "/Volumes/c/s/fs/0f1e2d"
    assert FM.path_of(type("Ref", (), {"uri": uri})()) == "/Volumes/c/s/fs/0f1e2d"
    # a STRING is joined with the root; an absolute one is kept
    assert (
        FM.path_of("data/a.zpack", "/Volumes/c/s/v/r")
        == "/Volumes/c/s/v/r/data/a.zpack"
    )
    assert FM.path_of("/Volumes/x/a.zpack", "/Volumes/c/s/v/r") == "/Volumes/x/a.zpack"


def test_stored_files_are_relative_to_the_root_each_row_lies_under():
    layout = FM.FileLayout("external", gbx_root="/own", base_root="/base")
    rows = pa.table(
        {
            "file": [
                "dbfs:/base/t/c/0",
                "dbfs:/own/data/a/w/p.zpack",
                "dbfs:/else/x",
                None,
            ],
            "origin": ["external", "gbx", "gbx", None],
        }
    )
    out = M._stored_files(rows, layout).column("file").to_pylist()
    assert out == ["t/c/0", "data/a/w/p.zpack", "/else/x", None]
    assert M.file_select(layout) == "file.uri AS file"
    assert M.file_select(FM.FileLayout()) == "file"
    assert M._stored_files(rows, FM.FileLayout()) is rows


def test_a_reader_opens_a_file_uri_as_its_bare_path(tmp_path):
    (tmp_path / "p.zpack").write_bytes(b"0123456789")
    reader = FuseRangeReader(str(tmp_path))
    region = Region(f"dbfs:{tmp_path}/p.zpack", 2, 4, "gbx")
    assert reader.read([region]) == [b"2345"]


# --- the manifest paths end to end, FILE emulated -------------------------------------------


def test_external_mode_commits_and_reads_files_as_root_relative_paths(
    spark_delta, tmp_path, monkeypatch
):
    """``file`` is a one-field struct and ``try_to_file`` a function that answers for files
    that exist, as on a FILE-enabled cluster: the commit stores the reference, reading and
    the vacuum's file lists give back the relative path, a missing file aborts the commit.
    """
    from pyspark.sql import types as T

    root = tmp_path / "store"
    (root / "data" / "t" / "w1").mkdir(parents=True)
    pack = root / "data" / "t" / "w1" / "p.zpack"
    pack.write_bytes(b"x" * 16)
    uri_type = T.StructType([T.StructField("uri", T.StringType())])
    spark_delta.udf.register(
        "try_to_file",
        lambda p: ({"uri": "dbfs:" + p} if p and os.path.isfile(p) else None),
        uri_type,
    )
    monkeypatch.setattr(
        M,
        "file_column_type",
        lambda mode: "STRUCT<uri: STRING>" if mode != "fuse" else "STRING",
    )
    table = f"zf_{uuid.uuid4().hex[:8]}"
    M.create_manifest(spark_delta, table, _props("external", root=str(root)))

    def chunk(key, wid, file):
        row = dict(
            kind="chunk",
            array="t",
            key=key,
            shard_key="c/0",
            file=file,
            offset=0,
            length=8,
            checksum="00",
            origin="gbx",
            write_id=wid,
            op="upsert",
        )
        return pa.Table.from_pylist([row], schema=M._COMMIT_SCHEMA)

    try:
        w1 = uuid.uuid4().hex
        version = M.commit(
            spark_delta,
            table,
            chunk("c/0", w1, "data/t/w1/p.zpack"),
            write_id=w1,
            meta={},
        )
        stored = spark_delta.sql(
            f"SELECT file.uri FROM {table} WHERE kind = 'chunk'"
        ).first()[0]
        assert stored == f"dbfs:{root}/data/t/w1/p.zpack"
        rows = M.read_rows(spark_delta, M.snapshot(spark_delta, table), kinds="chunk")
        assert rows.column("file").to_pylist() == ["data/t/w1/p.zpack"]
        assert MT._files_at(spark_delta, table, version, str(root)) == {
            "data/t/w1/p.zpack"
        }

        w2 = uuid.uuid4().hex
        with pytest.raises(ZarrStoreDrift, match="not a file") as e:
            M.commit(
                spark_delta,
                table,
                chunk("c/1", w2, "data/t/w2/gone.zpack"),
                write_id=w2,
                meta={},
            )
        assert "Fix:" in str(e.value)
        assert M.find_commit_version(spark_delta, table, w2) is None
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {table}")
        spark_delta.sql("DROP TEMPORARY FUNCTION IF EXISTS try_to_file")
