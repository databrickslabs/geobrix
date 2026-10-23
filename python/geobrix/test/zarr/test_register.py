"""``gbx_zarr_register``: a manifest over a store's own objects, nothing copied."""

import json
import os
import shutil
import uuid

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest

zarr = pytest.importorskip("zarr")

from databricks.labs.gbx.zarr import _register  # noqa: E402
from databricks.labs.gbx.zarr._access import FuseRangeReader  # noqa: E402
from databricks.labs.gbx.zarr._manifest import (  # noqa: E402
    read_props,
    read_rows,
    snapshot,
)
from databricks.labs.gbx.zarr._meta import parse_v2, parse_v3  # noqa: E402
from databricks.labs.gbx.zarr._register import (  # noqa: E402
    OWNER_MARKER,
    gbx_zarr_register,
    list_objects,
    read_base_metadata,
    write_owner_marker,
)
from databricks.labs.gbx.zarr._store import ManifestStore  # noqa: E402
from databricks.labs.gbx.zarr._testing import plain_store_rows  # noqa: E402
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrError,
    ZarrOwnershipError,
    ZarrUnsupported,
)


def _table(label):
    return f"gbx_zarr_reg_{label}_{uuid.uuid4().hex[:8]}"


def _exists(spark, table):
    return spark.catalog.tableExists(table)


def _live(spark, table, **kw):
    return read_rows(spark, snapshot(spark, table), **kw)


def _store_key(array, key):
    return "/".join(p for p in (array, key) if p)


def _open_store(spark, table, base, scratch):
    """A ``ManifestStore`` made only from what the manifest holds (no look at ``base``)."""
    rows = _live(spark, table).to_pylist()
    metas = {
        _store_key(r["array"], r["key"]): r["meta"].encode()
        for r in rows
        if r["kind"] in ("group", "array")
    }
    arrays = {}
    for key, raw in metas.items():
        prefix, _, name = key.rpartition("/")
        if not prefix:
            continue
        if name == "zarr.json" and json.loads(raw).get("node_type") == "array":
            arrays[prefix] = parse_v3(prefix, json.loads(raw))
        elif name == ".zarray":
            zattrs = json.loads(metas.get(f"{prefix}/.zattrs", b"{}"))
            arrays[prefix] = parse_v2(prefix, json.loads(raw), zattrs)
    reader = FuseRangeReader(str(scratch), base_root=str(base))
    return ManifestStore(metas, rows, reader, arrays), arrays


def _assert_reads_like_the_base(store, arrays, base):
    assert arrays
    for name in arrays:
        got = zarr.open_array(store=store, path=name, mode="r")[...]
        want = zarr.open_array(str(base / name), mode="r")[...]
        np.testing.assert_array_equal(got, want, err_msg=name)


def _tree(root):
    """Every file and directory under ``root`` with its size and mtime."""
    out = {}
    for dirpath, dirs, files in os.walk(root):
        for n in dirs + files:
            p = os.path.join(dirpath, n)
            st = os.stat(p)
            out[os.path.relpath(p, root)] = (
                st.st_size if os.path.isfile(p) else None,
                st.st_mtime_ns,
            )
    return out


# --- the five contract tests ------------------------------------------------------


def test_register_v3_sharded_rows_match_objects(spark_delta, era5_v3_sharded, tmp_path):
    table = _table("v3s")
    version = gbx_zarr_register(spark_delta, str(era5_v3_sharded), table)
    assert isinstance(version, int)
    assert version == snapshot(spark_delta, table).version

    shards = _live(spark_delta, table, kinds="shard")
    on_disk = [
        f
        for arr in ("t2m", "u10")
        for f in (era5_v3_sharded / arr / "c").rglob("*")
        if f.is_file()
    ]
    assert shards.num_rows == len(on_disk) == 4
    assert set(shards.column("array").to_pylist()) == {"t2m", "u10"}
    assert pc.all(pc.equal(shards["key"], shards["shard_key"])).as_py()

    chunks = _live(spark_delta, table, kinds="chunk")  # the unsharded coordinates
    assert set(chunks.column("array").to_pylist()) == {"time", "latitude", "longitude"}
    assert pc.all(pc.is_null(chunks["shard_key"])).as_py()

    for r in [*shards.to_pylist(), *chunks.to_pylist()]:
        f = era5_v3_sharded / r["file"]
        st = f.stat()
        assert r["file"] == f"{r['array']}/{r['key']}"
        assert (r["offset"], r["length"]) == (0, st.st_size)
        assert r["origin"] == "external"
        assert r["identity"] == f"{st.st_mtime_ns // 1_000_000}:{st.st_size}"

    commit = _live(spark_delta, table, kinds="commit").to_pylist()
    assert len(commit) == 1
    assert json.loads(commit[0]["meta"]) == {
        "mode": "register",
        "objects": shards.num_rows + chunks.num_rows,
    }

    store, arrays = _open_store(spark_delta, table, era5_v3_sharded, tmp_path)
    assert set(arrays) == {"t2m", "u10", "time", "latitude", "longitude"}
    _assert_reads_like_the_base(store, arrays, era5_v3_sharded)


def test_register_v2_unsharded(spark_delta, era5_v2, tmp_path):
    table = _table("v2")
    gbx_zarr_register(spark_delta, str(era5_v2), table)

    rows = _live(spark_delta, table)
    assert set(rows.column("kind").to_pylist()) == {"group", "array", "chunk", "commit"}
    chunks = rows.filter(pc.equal(rows["kind"], "chunk"))
    assert pc.all(pc.is_null(chunks["shard_key"])).as_py()
    assert "0.0.0" in chunks.filter(pc.equal(chunks["array"], "t2m"))["key"].to_pylist()

    meta_keys = {
        _store_key(r["array"], r["key"])
        for r in rows.to_pylist()
        if r["kind"] in ("group", "array")
    }
    assert {".zgroup", ".zattrs", "t2m/.zarray", "t2m/.zattrs"} <= meta_keys

    store, arrays = _open_store(spark_delta, table, era5_v2, tmp_path)
    _assert_reads_like_the_base(store, arrays, era5_v2)


def test_register_writable_requires_overlay_root(spark_delta, era5_v3_unsharded):
    table = _table("nooverlay")
    with pytest.raises(ValueError, match="overlay_root"):
        gbx_zarr_register(spark_delta, str(era5_v3_unsharded), table, writable=True)
    with pytest.raises(ValueError, match="overlay_root"):
        gbx_zarr_register(
            spark_delta, str(era5_v3_unsharded), table, overlay_root="/tmp/x"
        )
    assert not _exists(spark_delta, table)


def test_overlay_root_exclusive(spark_delta, era5_v3_unsharded, tmp_path):
    first, second = _table("own1"), _table("own2")
    overlay = tmp_path / "overlay"
    gbx_zarr_register(
        spark_delta,
        str(era5_v3_unsharded),
        first,
        writable=True,
        overlay_root=str(overlay),
    )
    marker = json.loads((overlay / OWNER_MARKER).read_text())
    assert marker["table"] == first and marker["kind"] == "overlay"

    with pytest.raises(ZarrOwnershipError) as ei:
        gbx_zarr_register(
            spark_delta,
            str(era5_v3_unsharded),
            second,
            writable=True,
            overlay_root=str(overlay),
        )
    assert first in str(ei.value) and "Fix:" in str(ei.value)
    assert not _exists(spark_delta, second)
    assert json.loads((overlay / OWNER_MARKER).read_text())["table"] == first

    write_owner_marker(str(overlay), first, "overlay")  # its own table: no error


def test_register_never_writes_into_base(spark_delta, era5_v3_sharded, tmp_path):
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v3_sharded, base)
    before = _tree(base)

    gbx_zarr_register(spark_delta, str(base), _table("ro"))
    gbx_zarr_register(
        spark_delta,
        str(base),
        _table("rw"),
        writable=True,
        overlay_root=str(tmp_path / "overlay"),
    )

    assert _tree(base) == before
    assert not (base / OWNER_MARKER).exists()


# --- what the table records -------------------------------------------------------


def test_properties_record_a_read_only_registration(spark_delta, era5_v3_unsharded):
    table = _table("props")
    gbx_zarr_register(spark_delta, str(era5_v3_unsharded), table, retain="2 days")
    props = read_props(spark_delta, table)
    assert (props.kind, props.root) == ("registered", str(era5_v3_unsharded))
    assert (props.writable, props.overlay_root) == (False, None)
    assert (props.file_mode, props.retain) == ("fuse", "2 days")


def test_properties_record_a_writable_registration(
    spark_delta, era5_v3_unsharded, tmp_path
):
    table = _table("propsrw")
    gbx_zarr_register(
        spark_delta,
        str(era5_v3_unsharded),
        table,
        writable=True,
        overlay_root=str(tmp_path / "ov"),
        file_mode="fuse",
    )
    props = read_props(spark_delta, table)
    assert (props.writable, props.overlay_root) == (True, str(tmp_path / "ov"))
    assert props.file_mode == "fuse"


@pytest.mark.parametrize("mode", ["external", "managed"])
def test_file_modes_that_need_file_columns_are_not_available_yet(
    spark_delta, era5_v3_unsharded, mode
):
    table = _table(mode)
    with pytest.raises(ZarrUnsupported) as ei:
        gbx_zarr_register(spark_delta, str(era5_v3_unsharded), table, file_mode=mode)
    assert "Fix:" in str(ei.value) and "fuse" in ei.value.fix
    assert not _exists(spark_delta, table)


def test_an_unknown_file_mode_is_a_value_error(spark_delta, era5_v3_unsharded):
    with pytest.raises(ValueError, match="file_mode"):
        gbx_zarr_register(
            spark_delta, str(era5_v3_unsharded), _table("bad"), file_mode="s3"
        )


# --- refusals leave nothing behind ------------------------------------------------


def test_an_existing_table_is_refused_before_any_marker_is_written(
    spark_delta, era5_v3_unsharded, tmp_path
):
    table = _table("again")
    gbx_zarr_register(spark_delta, str(era5_v3_unsharded), table)
    overlay = tmp_path / "ov"
    with pytest.raises(ZarrOwnershipError):
        gbx_zarr_register(
            spark_delta,
            str(era5_v3_unsharded),
            table,
            writable=True,
            overlay_root=str(overlay),
        )
    assert not (overlay / OWNER_MARKER).exists()


@pytest.mark.parametrize("where", ["inside", "around", "same"])
def test_an_overlay_that_overlaps_the_base_is_refused(
    spark_delta, era5_v3_unsharded, tmp_path, where
):
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v3_unsharded, base)
    overlay = {
        "inside": base / "overlay",
        "around": tmp_path,
        "same": base,
    }[where]
    before = _tree(base)
    table = _table("overlap")
    with pytest.raises(ZarrOwnershipError) as ei:
        gbx_zarr_register(
            spark_delta, str(base), table, writable=True, overlay_root=str(overlay)
        )
    assert "Fix:" in str(ei.value)
    assert not _exists(spark_delta, table)
    assert _tree(base) == before


def test_a_directory_that_is_not_a_store_is_refused(spark_delta, tmp_path):
    (tmp_path / "plain").mkdir()
    (tmp_path / "plain" / "notes.txt").write_text("hello")
    for target in (tmp_path / "plain", tmp_path / "missing"):
        table = _table("nostore")
        with pytest.raises(ZarrError) as ei:
            gbx_zarr_register(spark_delta, str(target), table)
        assert "Fix:" in str(ei.value)
        assert not _exists(spark_delta, table)


def test_a_path_that_is_not_on_a_volume_is_refused(spark_delta):
    with pytest.raises(ZarrUnsupported) as ei:
        gbx_zarr_register(spark_delta, "s3://bucket/cube.zarr", _table("s3"))
    assert "Fix:" in str(ei.value)


def test_a_failed_commit_leaves_no_empty_table(
    spark_delta, era5_v3_unsharded, monkeypatch
):
    def boom(*a, **k):
        raise RuntimeError("commit failed")

    monkeypatch.setattr(_register, "commit", boom)
    table = _table("boom")
    with pytest.raises(RuntimeError, match="commit failed"):
        gbx_zarr_register(spark_delta, str(era5_v3_unsharded), table)
    assert not _exists(spark_delta, table)


# --- stores that are not tidy -----------------------------------------------------


def test_zero_byte_and_stray_objects_do_not_break_the_store(
    spark_delta, era5_v3_unsharded, tmp_path, caplog
):
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v3_unsharded, base)
    hole = base / "t2m" / "c" / "1" / "0" / "0"
    hole.write_bytes(b"")  # a chunk that was truncated to nothing
    (base / "README.txt").write_text("vendor notes")
    (base / "t2m" / "c" / "00" / "0" / "0").parent.mkdir(parents=True)
    (base / "t2m" / "c" / "00" / "0" / "0").write_bytes(b"not a canonical key")
    (base / "t2m" / "thumbs.db").write_bytes(b"x")

    table = _table("messy")
    with caplog.at_level("WARNING"):
        gbx_zarr_register(spark_delta, str(base), table)

    rows = _live(spark_delta, table, kinds="chunk")
    assert pc.all(pc.greater(rows["length"], 0)).as_py()
    t2m_keys = set(rows.filter(pc.equal(rows["array"], "t2m"))["key"].to_pylist())
    assert "c/1/0/0" not in t2m_keys and "c/0/0/0" in t2m_keys
    assert not any(
        "README" in f or "00" in f.split("/") for f in rows["file"].to_pylist()
    )

    store, _ = _open_store(
        spark_delta, table, base, tmp_path
    )  # no drift at construction
    got = zarr.open_array(store=store, path="t2m", mode="r")
    want = zarr.open_array(str(era5_v3_unsharded / "t2m"), mode="r")[:]
    np.testing.assert_array_equal(got[:12], want[:12])  # chunks that are there
    hole = got[12:24, :37, :72]  # the truncated chunk reads as the fill value
    np.testing.assert_array_equal(hole, np.full(hole.shape, got.fill_value, hole.dtype))


def test_rows_for_empty_and_stray_objects(era5_v3_sharded):
    """The row builder, given what a listing might return, whatever Spark drops."""
    _, _, arrays = plain_store_rows(era5_v3_sharded)
    objects = pa.table(
        {
            "relpath": [
                "t2m/c/0/0/0",  # a shard
                "t2m/c/1/0/0",  # a shard, but empty
                "t2m/c/00/0/0",  # not a canonical key
                "t2m/c/0/0",  # wrong number of dimensions
                "t2m/zarr.json",  # metadata
                "zarr.json",  # metadata
                "elsewhere/data.bin",  # no array
                "time/c/0",  # an unsharded chunk
            ],
            "length": [10, 0, 5, 5, 7, 7, 5, 8],
            "mtime_ms": [1, 2, 3, 4, 5, 6, 7, 8],
        }
    )
    rows, counts = _register._chunk_rows(objects, arrays, "a" * 32)
    got = {(r["kind"], r["array"], r["key"]): r for r in rows.to_pylist()}
    assert set(got) == {("shard", "t2m", "c/0/0/0"), ("chunk", "time", "c/0")}
    shard = got[("shard", "t2m", "c/0/0/0")]
    assert (shard["shard_key"], shard["identity"], shard["length"]) == (
        "c/0/0/0",
        "1:10",
        10,
    )
    assert got[("chunk", "time", "c/0")]["shard_key"] is None
    assert (counts["empty"], counts["stray"]) == (1, 3)


def test_nested_groups_and_hidden_names_in_an_unusual_path(spark_delta, tmp_path):
    base = tmp_path / "_my data [v1]" / "cube.zarr"
    g = zarr.open_group(str(base), mode="w", zarr_format=3)
    fc = g.create_group("fc")
    x = fc.create_array("x", shape=(4, 6), chunks=(2, 3), dtype="f4", fill_value=0.0)
    x[:] = np.arange(24, dtype="f4").reshape(4, 6)
    aux = fc.create_array("_aux", shape=(5,), chunks=(2,), dtype="i4", fill_value=0)
    aux[:] = np.arange(5, dtype="i4")
    crs = g.create_array("crs", shape=(), chunks=(), dtype="i4", fill_value=0)
    crs[...] = 4326  # a scalar variable, as CF stores carry for the grid mapping

    table = _table("nested")
    gbx_zarr_register(spark_delta, str(base), table)

    chunks = _live(spark_delta, table, kinds="chunk")
    by_array = {
        a: sorted(chunks.filter(pc.equal(chunks["array"], a))["key"].to_pylist())
        for a in ("fc/x", "fc/_aux")
    }
    assert len(by_array["fc/x"]) == 4  # 2 x 2 chunks
    assert by_array["fc/_aux"] == ["c/0", "c/1", "c/2"]  # Spark skips "_" names

    groups = {
        _store_key(r["array"], r["key"])
        for r in _live(spark_delta, table, kinds="group").to_pylist()
    }
    assert groups == {"zarr.json", "fc/zarr.json"}

    store, arrays = _open_store(spark_delta, table, base, tmp_path / "scratch")
    assert set(arrays) == {"fc/x", "fc/_aux", "crs"}
    _assert_reads_like_the_base(store, arrays, base)


def test_consolidated_v2_metadata_is_kept_and_used(
    spark_delta, era5_v2, tmp_path, monkeypatch
):
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v2, base)
    zarr.consolidate_metadata(str(base))
    assert (base / ".zmetadata").exists()

    metas, arrays = read_base_metadata(str(base))
    assert ".zmetadata" in metas and "t2m/.zarray" in metas
    assert set(arrays) == {"t2m", "u10", "time", "latitude", "longitude"}

    table = _table("consol")
    gbx_zarr_register(spark_delta, str(base), table)
    keys = {
        _store_key(r["array"], r["key"])
        for r in _live(spark_delta, table, kinds=["group", "array"]).to_pylist()
    }
    assert ".zmetadata" in keys and "t2m/.zarray" in keys
    store, arrays = _open_store(spark_delta, table, base, tmp_path)
    _assert_reads_like_the_base(store, arrays, base)


def test_metadata_documents_are_kept_byte_for_byte(era5_v3_sharded):
    metas, arrays = read_base_metadata(str(era5_v3_sharded))
    assert set(metas) == {
        "zarr.json",
        *(f"{a}/zarr.json" for a in ("t2m", "u10", "time", "latitude", "longitude")),
    }
    for key, raw in metas.items():
        assert raw == (era5_v3_sharded / key).read_bytes()
    assert arrays["t2m"].shard is not None and arrays["time"].shard is None


# --- the listing ------------------------------------------------------------------


def test_list_objects_reports_each_object_once_with_its_identity(
    spark_delta, era5_v3_sharded
):
    objects = list_objects(spark_delta, str(era5_v3_sharded))
    assert objects.column_names == ["relpath", "length", "mtime_ms"]
    assert objects["relpath"].to_pylist() == sorted(objects["relpath"].to_pylist())

    plain = {
        f.relative_to(era5_v3_sharded).as_posix(): f.stat()
        for f in era5_v3_sharded.rglob("*")
        if f.is_file()
    }
    got = {r["relpath"]: r for r in objects.to_pylist()}
    assert set(got) == set(plain)
    for rel, st in plain.items():
        assert got[rel]["length"] == st.st_size
        assert got[rel]["mtime_ms"] == st.st_mtime_ns // 1_000_000


def test_a_store_with_no_chunks_registers_its_metadata_only(spark_delta, tmp_path):
    base = tmp_path / "empty.zarr"
    g = zarr.open_group(str(base), mode="w", zarr_format=3)
    g.create_array("x", shape=(4,), chunks=(2,), dtype="f4", fill_value=0.0)
    table = _table("nochunks")
    gbx_zarr_register(spark_delta, str(base), table)
    rows = _live(spark_delta, table)
    assert set(rows.column("kind").to_pylist()) == {"group", "array", "commit"}


# --- the testing helper agrees with register --------------------------------------


@pytest.mark.parametrize("fixture", ["era5_v3_sharded", "era5_v3_unsharded", "era5_v2"])
def test_plain_store_rows_match_the_registered_rows(spark_delta, fixture, request):
    root = request.getfixturevalue(fixture)
    table = _table("parity")
    gbx_zarr_register(spark_delta, str(root), table)
    fields = ("kind", "array", "key", "shard_key", "file", "offset", "length")
    got = sorted(
        (
            tuple(r[f] for f in fields)
            for r in _live(spark_delta, table, kinds=["shard", "chunk"]).to_pylist()
        ),
        key=repr,
    )
    _, rows, _ = plain_store_rows(root, origin="external")
    want = sorted((tuple(r[f] for f in fields) for r in rows), key=repr)
    assert got == want
