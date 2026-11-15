"""File modes: which mode a call gets, the DDL and MERGE each mode emits, and how a
FILE value maps back to the path string the rest of the store uses.

FILE does not exist in OSS Spark, so these use a fake probe and assert the SQL text. The
one end-to-end test emulates the FILE type with a one-field struct and a ``try_to_file``
function, to run the manifest's own commit and read paths; what FILE itself does is checked
on a FILE-enabled cluster.
"""

import os
import uuid

import numpy as np
import pyarrow as pa
import pytest

pytest.importorskip("zarr")
pytest.importorskip("xarray")

from test.zarr import _helpers as H  # noqa: E402

from databricks.labs.gbx.zarr import _filemode as FM  # noqa: E402
from databricks.labs.gbx.zarr import _maint as MT  # noqa: E402
from databricks.labs.gbx.zarr import _manifest as M  # noqa: E402
from databricks.labs.gbx.zarr._access import FuseRangeReader  # noqa: E402
from databricks.labs.gbx.zarr._register import (  # noqa: E402
    OWNER_MARKER,
    gbx_zarr_register,
)
from databricks.labs.gbx.zarr._shard import Region  # noqa: E402
from databricks.labs.gbx.zarr._testing import era5_like_dataset  # noqa: E402
from databricks.labs.gbx.zarr._write import gbx_zarr_write  # noqa: E402
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrStoreDrift,
    ZarrUnsupported,
)

# the gate's two texts
NOT_VERIFIED = "were not verified here|not available in this release"


@pytest.fixture
def file_support(monkeypatch):
    """A runtime with FILE, and the verified modes set to ``modes`` (none: the gate)."""

    def make(supported=True, modes=("external", "managed")):
        monkeypatch.setattr(FM, "file_supported", lambda spark: supported)
        monkeypatch.setattr(FM, "VERIFIED_FILE_MODES", frozenset(modes))

    return make


@pytest.fixture
def fake_file_type(spark_delta, monkeypatch):
    """FILE emulated on OSS Delta: ``file`` is a one-field struct and ``try_to_file`` a
    function that answers for a file that exists, as it does on a FILE-enabled cluster.
    """
    from pyspark.sql import types as T

    spark_delta.udf.register(
        "try_to_file",
        lambda p: ({"uri": "dbfs:" + p} if p and os.path.isfile(p) else None),
        T.StructType([T.StructField("uri", T.StringType())]),
    )
    monkeypatch.setattr(
        M,
        "file_column_type",
        lambda mode: "STRUCT<uri: STRING>" if mode != "fuse" else "STRING",
    )
    yield
    spark_delta.sql("DROP TEMPORARY FUNCTION IF EXISTS try_to_file")


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
        (True, ()),  # FILE, but the gate has no mode open
    ],
)
def test_auto_resolves_to_fuse_without_file_support(file_support, supported, modes):
    file_support(supported, modes)
    for filespace in (True, False):
        assert FM.resolve(None, "auto", has_filespace=filespace) == "fuse"
    assert FM.resolve(None, "fuse", has_filespace=True) == "fuse"


def test_external_file_mode_is_open_and_managed_is_refused(monkeypatch):
    """On a FILE-enabled cluster FILE EXTERNAL passed and FILE MANAGED failed (Delta VACUUM never reclaims
    a managed file's bytes): `auto` picks external on a FILE runtime, and managed is refused by
    name. Where FILE is not supported `auto` stays on fuse."""
    assert FM.VERIFIED_FILE_MODES == frozenset({"external"})
    monkeypatch.setattr(FM, "file_supported", lambda spark: True)
    for filespace in (True, False):
        assert FM.resolve(None, "auto", has_filespace=filespace) == "external"
    assert FM.resolve(None, "external", has_filespace=True) == "external"
    with pytest.raises(ZarrUnsupported, match="not available in this release") as e:
        FM.resolve(None, "managed", has_filespace=True)
    assert "reclaim" in str(e.value) and "Fix:" in str(e.value)
    monkeypatch.setattr(FM, "file_supported", lambda spark: False)  # DCS, no preview
    assert FM.resolve(None, "auto", has_filespace=True) == "fuse"


@pytest.mark.parametrize("mode", ["external", "managed"])
@pytest.mark.parametrize(
    "supported, modes, text",
    [
        (True, (), NOT_VERIFIED),  # the gate closed, even on a FILE runtime
        (False, ("external", "managed"), "does not support FILE"),
    ],
)
def test_explicit_managed_raises_without_file_support(
    file_support, mode, supported, modes, text
):
    file_support(supported, modes)
    with pytest.raises(ZarrUnsupported, match=text) as e:
        FM.resolve(None, mode, has_filespace=True)
    assert "use file_mode=" in e.value.fix and "Fix:" in str(e.value)


def test_a_verified_runtime_picks_the_mode_by_its_filespace(file_support, monkeypatch):
    file_support()
    # managed is verified here, but GeoBrix does not write its files: auto never picks it
    assert FM.resolve(None, "auto", has_filespace=True) == "external"
    assert FM.resolve(None, "auto", has_filespace=False) == "external"
    monkeypatch.setattr(FM, "GEOBRIX_FILE_MODES", ("fuse", "external", "managed"))
    assert FM.resolve(None, "auto", has_filespace=True) == "managed"
    assert FM.resolve(None, "auto", has_filespace=False) == "external"
    assert FM.resolve(None, "managed", has_filespace=False) == "managed"
    with pytest.raises(ValueError, match=r"file_mode.*Fix:"):
        FM.resolve(None, "s3", has_filespace=False)


def test_a_mode_geobrix_does_not_write_is_refused_by_name_on_create(file_support):
    file_support()
    assert FM.resolve(None, "managed", has_filespace=True) == "managed"  # verified
    with pytest.raises(ZarrUnsupported, match="file_mode='managed'") as e:
        FM.require_geobrix_files("t", "managed", "write", new=True)
    assert e.value.fix == 'pass file_mode="auto" or "fuse"'
    with pytest.raises(ZarrUnsupported, match="keeps its files") as e:
        FM.require_geobrix_files("t", "managed", "write")
    assert "store with file_mode" in e.value.fix


def test_registered_base_never_managed(file_support):
    file_support()  # even where managed is verified and supported
    with pytest.raises(ZarrUnsupported, match="duplicated") as e:
        gbx_zarr_register(None, "/Volumes/c/s/v/base", "t", file_mode="managed")
    assert e.value.fix == "use the default file mode"  # not a mode that may be refused
    # a registered store gets no filespace, an overlay included: external, else fuse
    assert FM.resolve(None, "auto", has_filespace=True, registered=True) == "external"
    with pytest.raises(ValueError, match="registered"):
        M.create_manifest(None, "t", _props("managed", kind="registered", root="/base"))


@pytest.mark.parametrize("entry", ["write", "register"])
@pytest.mark.parametrize(
    "given, want", [({}, "fuse"), ({"file_mode": "auto"}, "external")]
)
def test_the_default_file_mode_is_fuse_and_auto_opts_into_file(
    spark_delta,
    tmp_path,
    era5_v3_unsharded,
    file_support,
    fake_file_type,
    entry,
    given,
    want,
):
    """Where FILE works, a store made with no ``file_mode`` still keeps path strings, so it
    reads on every compute; ``file_mode="auto"`` is what picks FILE references there."""
    file_support()
    table = f"zf_{uuid.uuid4().hex[:8]}"
    try:
        if entry == "write":
            data = era5_like_dataset().isel(time=slice(0, 12), latitude=slice(0, 5))
            gbx_zarr_write(
                data, table, root=str(tmp_path / "store"), spark=spark_delta, **given
            )
        else:
            gbx_zarr_register(spark_delta, str(era5_v3_unsharded), table, **given)
        assert M.read_props(spark_delta, table).file_mode == want
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    "entry",
    [
        "read",
        "write",
        "sync",
        "publish",
        "optimize",
        "vacuum",
        "restore",
        "tag",
        "untag",
        "history",
        "export",
    ],
)
def test_a_file_store_is_refused_on_compute_without_file(
    spark_delta, tmp_path, fake_file_type, monkeypatch, entry
):
    """A store made in FILE mode on a runtime with the File type preview is used only
    where FILE values work: every entry point refuses with a Fix, before it writes a file.
    """
    from databricks.labs.gbx.zarr import _sync as SY
    from databricks.labs.gbx.zarr._export import gbx_zarr_export
    from databricks.labs.gbx.zarr._read import gbx_zarr_read

    root = tmp_path / "store"
    root.mkdir()
    table = f"zf_{uuid.uuid4().hex[:8]}"
    M.create_manifest(spark_delta, table, _props("external", root=str(root)))
    data = era5_like_dataset().isel(time=slice(0, 12), latitude=slice(0, 5))
    calls = {
        "read": lambda: gbx_zarr_read(spark_delta, table),
        "write": lambda: gbx_zarr_write(data, table, mode="append", spark=spark_delta),
        "sync": lambda: SY.gbx_zarr_sync(spark_delta, table),
        "publish": lambda: SY.publish(spark_delta, table),
        "optimize": lambda: MT.gbx_zarr_optimize(spark_delta, table),
        "vacuum": lambda: MT.gbx_zarr_vacuum(spark_delta, table),
        "restore": lambda: MT.gbx_zarr_restore(spark_delta, table, version_as_of=0),
        "tag": lambda: MT.gbx_zarr_tag(spark_delta, table, "v1"),
        "untag": lambda: MT.gbx_zarr_untag(spark_delta, table, "v1"),
        "history": lambda: MT.gbx_zarr_history(spark_delta, table),
        "export": lambda: gbx_zarr_export(spark_delta, table, str(tmp_path / "copy")),
    }
    monkeypatch.setattr(FM, "file_supported", lambda spark: False)
    try:
        with pytest.raises(ZarrUnsupported, match="FILE values") as e:
            calls[entry]()
        assert "Fix:" in str(e.value)
        for named in ("Runtime 19 dedicated", "fileReferenceCreationMode", "'fuse'"):
            assert named in e.value.fix
        assert list(root.iterdir()) == [] and not (tmp_path / "copy").exists()
        monkeypatch.setattr(FM, "file_supported", lambda spark: True)
        M.read_props(spark_delta, table, need_file=True)  # FILE compute: no refusal
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {table}")


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

    # a FILE value is read as its uri, never collected whole
    assert M.file_select(layout) == "file.uri AS file"
    assert M.file_select(FM.FileLayout()) == "file"


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


def test_a_reader_opens_a_file_uri_as_its_bare_path(tmp_path):
    (tmp_path / "p.zpack").write_bytes(b"0123456789")
    reader = FuseRangeReader(str(tmp_path))
    region = Region(f"dbfs:{tmp_path}/p.zpack", 2, 4, "gbx")
    assert reader.read([region]) == [b"2345"]


# --- the manifest paths end to end, FILE emulated -------------------------------------------


def _chunk_row(key, wid, file, origin="gbx"):
    row = dict(
        kind="chunk",
        array="t",
        key=key,
        shard_key="c/0",
        file=file,
        offset=0,
        length=8,
        checksum="00",
        origin=origin,
        write_id=wid,
        op="upsert",
    )
    return pa.Table.from_pylist([row], schema=M._COMMIT_SCHEMA)


def test_external_mode_commits_and_reads_files_as_root_relative_paths(
    spark_delta, tmp_path, fake_file_type
):
    """The commit stores the reference, reading and the vacuum's file lists give back the
    path relative to the root each row lies under (a registered store has two), and a
    file that is gone aborts the commit."""
    root, base, overlay = (tmp_path / n for n in ("store", "base", "overlay"))
    for d, name in ((root, "data/t/w1/p.zpack"), (base, "t/c/0"), (overlay, "p.zpack")):
        (d / name).parent.mkdir(parents=True)
        (d / name).write_bytes(b"x" * 16)
    plain = f"zf_{uuid.uuid4().hex[:8]}"
    registered = f"zr_{uuid.uuid4().hex[:8]}"
    M.create_manifest(spark_delta, plain, _props("external", root=str(root)))
    M.create_manifest(
        spark_delta,
        registered,
        _props(
            "external", kind="registered", root=str(base), overlay_root=str(overlay)
        ),
    )
    try:
        w1 = uuid.uuid4().hex
        version = M.commit(
            spark_delta,
            plain,
            _chunk_row("c/0", w1, "data/t/w1/p.zpack"),
            write_id=w1,
            meta={},
        )
        stored = spark_delta.sql(
            f"SELECT file.uri FROM {plain} WHERE kind = 'chunk'"
        ).first()[0]
        assert stored == f"dbfs:{root}/data/t/w1/p.zpack"
        rows = M.read_rows(spark_delta, M.snapshot(spark_delta, plain), kinds="chunk")
        assert rows.column("file").to_pylist() == ["data/t/w1/p.zpack"]
        assert MT._files_at(spark_delta, plain, version, str(root)) == {
            "data/t/w1/p.zpack"
        }

        w2 = uuid.uuid4().hex
        with pytest.raises(ZarrStoreDrift, match="not a file") as e:
            M.commit(
                spark_delta,
                plain,
                _chunk_row("c/1", w2, "data/t/w2/gone.zpack"),
                write_id=w2,
                meta={},
            )
        assert "Fix:" in str(e.value)
        assert M.find_commit_version(spark_delta, plain, w2) is None

        # a registered store: a base object is relative to the base, GeoBrix's to the overlay
        w3 = uuid.uuid4().hex
        both = pa.concat_tables(
            [
                _chunk_row("c/0", w3, "t/c/0", origin="external"),
                _chunk_row("c/1", w3, "p.zpack"),
            ]
        )
        M.commit(spark_delta, registered, both, write_id=w3, meta={})
        rows = M.read_rows(
            spark_delta, M.snapshot(spark_delta, registered), kinds="chunk"
        )
        got = {r["key"]: (r["file"], r["origin"]) for r in rows.to_pylist()}
        assert got == {"c/0": ("t/c/0", "external"), "c/1": ("p.zpack", "gbx")}
    finally:
        for t in (plain, registered):
            spark_delta.sql(f"DROP TABLE IF EXISTS {t}")


def test_an_external_store_takes_a_partial_update(
    spark_delta, tmp_path, file_support, fake_file_type
):
    """A write into an existing external store reads the base chunks it patches through the
    manifest's FILE column: ``_base_frame`` must hand the tasks the uri, not the struct.
    """
    file_support()
    ds = era5_like_dataset().isel(latitude=slice(0, 10), longitude=slice(0, 12))
    chunks = {"time": 12, "latitude": 5, "longitude": 6}
    table = f"zf_{uuid.uuid4().hex[:8]}"
    try:
        gbx_zarr_write(
            ds,
            table,
            root=str(tmp_path / "store"),
            spark=spark_delta,
            chunks=chunks,
            file_mode="external",
        )
        assert M.read_props(spark_delta, table).file_mode == "external"
        patch = np.full((3, 5, 6), 7.0, dtype="float32")
        gbx_zarr_write(
            H.blocks_frame(spark_delta, ("t2m", (0, 0, 0), patch)),
            table,
            mode="update",
            spark=spark_delta,
        )
        got = H.read_variable(spark_delta, table, "t2m")
        want = ds["t2m"].values.copy()
        want[:3, :5, :6] = 7.0
        np.testing.assert_array_equal(got, want)
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {table}")


def test_a_create_that_fails_leaves_no_owner_marker(
    spark_delta, tmp_path, file_support
):
    """Here the CREATE fails (OSS has no FILE type), as it can on a runtime without the
    preview: the marker the call claimed goes back, so the root is free for another store.
    """
    file_support()
    root = tmp_path / "store"
    ds = era5_like_dataset().isel(time=slice(0, 12), latitude=slice(0, 5))
    table = f"zf_{uuid.uuid4().hex[:8]}"
    with pytest.raises(Exception, match="(?i)EXTERNAL|FILE"):
        gbx_zarr_write(
            ds, table, root=str(root), spark=spark_delta, file_mode="external"
        )
    assert not (root / OWNER_MARKER).exists()
    assert not spark_delta.catalog.tableExists(table)
