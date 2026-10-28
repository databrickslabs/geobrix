"""``gbx_zarr_export``: a store version as a plain Zarr copy or a kerchunk reference file.

These run on local OSS Delta. The two store shapes every test covers: a managed Zarr v3
store with sharded arrays (a byte-copied shard, a shard assembled from a shard row plus
chunk rows, and one assembled from chunk rows alone), and a registered Zarr v2 store whose
vendor wrote consolidated metadata and whose overlay holds a tombstone and an append.
"""

import base64
import json
import os
import struct
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

from test.zarr import _helpers as H  # noqa: E402

from databricks.labs.gbx.zarr import _export, _maint  # noqa: E402
from databricks.labs.gbx.zarr._export import gbx_zarr_export  # noqa: E402
from databricks.labs.gbx.zarr._maint import (  # noqa: E402
    gbx_zarr_optimize,
    gbx_zarr_tag,
    gbx_zarr_vacuum,
)
from databricks.labs.gbx.zarr._register import gbx_zarr_register  # noqa: E402
from databricks.labs.gbx.zarr._testing import (  # noqa: E402
    era5_like_dataset,
    make_era5_like,
)
from databricks.labs.gbx.zarr._write import gbx_zarr_write  # noqa: E402
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrAccessUnavailable,
    ZarrUnsupported,
    ZarrVersionUnavailable,
)

CHUNKS = {"time": 12, "latitude": 5, "longitude": 6}
SHARDS = {"time": 24, "latitude": 10, "longitude": 12}  # two shards per variable
CHUNK = (12, 5, 6)
WHOLE = (12, 37, 72)  # one inner chunk of the registered cube
KINDS = ["managed", "registered_v2"]
# xarray's form of a v3 float _FillValue attribute: base64 of a little-endian float64
NAN_FILL = base64.standard_b64encode(struct.pack("<d", float("nan"))).decode()


def _table(label):
    return f"gbx_zarr_export_{label}_{uuid.uuid4().hex[:8]}"


def _cube():
    """48 hours on a 10 x 12 corner of the ERA5-like cube."""
    return era5_like_dataset().isel(latitude=slice(0, 10), longitude=slice(0, 12))


@pytest.fixture
def drop(spark_delta):
    made = []
    yield made.append
    for t in made:
        spark_delta.sql(f"DROP TABLE IF EXISTS {t}")


def _blocks(spark, name, start, shape, value):
    return H.blocks_frame(spark, (name, start, np.full(shape, value, dtype="float32")))


def _live(spark, table, kind, array="t2m"):
    return spark.sql(
        f"SELECT * FROM {table} WHERE kind = '{kind}' AND array = '{array}' "
        "AND tag IS NULL ORDER BY key"
    ).collect()


def _managed(spark, drop, tmp_path, ds=None, label="m", **kw):
    """A managed v3 store of the small cube (``t2m`` has a NaN ``_FillValue``)."""
    ds = _cube() if ds is None else ds
    if "_FillValue" not in ds["t2m"].encoding:
        ds["t2m"].encoding["_FillValue"] = np.float32("nan")
    table, root = _table(label), tmp_path / f"root_{label}"
    drop(table)
    kw.setdefault("chunks", CHUNKS)
    kw.setdefault("shards", SHARDS)
    version = gbx_zarr_write(ds, table, root=str(root), spark=spark, **kw)
    return table, root, version


def _scenario(spark, drop, tmp_path, kind):
    """``(table, files)``: a store with every row shape an export has to turn into objects.

    ``files`` maps a store key to the bytes the export must copy untouched.

    - managed: ``t2m`` is optimized (two shard rows), then chunk (0,0,0) is updated and chunk
      (12,0,0) is rewritten as all fill (a tombstone), both in shard ``c/0/0/0``; shard
      ``c/1/0/0`` is left whole. ``u10`` has chunk rows only.
    - registered_v2: the vendor store has consolidated metadata; the overlay holds a
      tombstone over the base chunk ``0.0.0`` and an append of 12 hours.
    """
    if kind == "managed":
        table, root, _ = _managed(spark, drop, tmp_path)
        gbx_zarr_optimize(spark, table, variables="t2m")
        gbx_zarr_write(
            _blocks(spark, "t2m", (0, 0, 0), CHUNK, 7.0), table, mode="update"
        )
        gbx_zarr_write(
            _blocks(spark, "t2m", (12, 0, 0), CHUNK, np.nan), table, mode="update"
        )
        assert [r["key"] for r in _live(spark, table, "shard")] == [
            "c/0/0/0",
            "c/1/0/0",
        ]
        assert len(_live(spark, table, "chunk")) == 2  # one live, one tombstone
        assert len(_live(spark, table, "chunk", "u10")) == 16
        whole = _live(spark, table, "shard")[1]
        pack = (root / whole["file"]).read_bytes()
        return table, {
            "t2m/c/1/0/0": pack[whole["offset"] : whole["offset"] + whole["length"]]
        }
    store = make_era5_like(tmp_path / "vendor.zarr", zarr_format=2)
    zarr.consolidate_metadata(
        str(store)
    )  # a copy of the documents, stale after the append
    table = _table("reg")
    drop(table)
    gbx_zarr_register(
        spark,
        str(store),
        table,
        writable=True,
        overlay_root=str(tmp_path / "overlay"),
    )
    gbx_zarr_write(
        _blocks(spark, "t2m", (0, 0, 0), WHOLE, np.nan), table, mode="update"
    )
    assert [r["file"] for r in _live(spark, table, "chunk") if r["key"] == "0.0.0"] == [
        None
    ]
    later = era5_like_dataset().isel(time=slice(0, 12))
    later = later.assign_coords(time=later.time.values + np.timedelta64(48, "h"))
    gbx_zarr_write(later, table, mode="append", spark=spark)
    return table, {"u10/0.0.0": (store / "u10" / "0.0.0").read_bytes()}


def _leave_a_plain_fill(spark, table):
    """Put back the ``t2m`` document as an older write left it, a plain ``"NaN"`` fill
    attribute, after checking that a write now stores xarray's form."""
    where = "kind = 'array' AND array = 't2m' AND tag IS NULL"
    doc = json.loads(
        spark.sql(f"SELECT meta FROM {table} WHERE {where}").first()["meta"]
    )
    assert doc["attributes"]["_FillValue"] == NAN_FILL
    doc["attributes"]["_FillValue"] = "NaN"
    spark.sql(
        f"UPDATE {table} SET meta = :meta WHERE {where}", args={"meta": json.dumps(doc)}
    )


def _wants(spark, table, names=("t2m", "u10"), **kw):
    return {n: H.read_variable(spark, table, n, **kw) for n in names}


def _tensorstore(path, fmt):
    import tensorstore as ts

    spec = {
        "driver": "zarr3" if fmt == 3 else "zarr",
        "kvstore": {"driver": "file", "path": str(path) + "/"},
    }
    return ts.open(spec).result().read().result()


def _assert_readers(copy, want, fmt):
    """The copy opens in xarray (consolidated metadata), zarr and tensorstore, and each
    returns the cells the manifest read returns."""
    with xr.open_zarr(str(copy), consolidated=True, chunks=None) as ds:
        for name, values in want.items():
            np.testing.assert_array_equal(ds[name].values, values)
    group = zarr.open_group(str(copy), mode="r", use_consolidated=True)
    for name, values in want.items():
        np.testing.assert_array_equal(group[name][...], values)
        np.testing.assert_array_equal(_tensorstore(copy / name, fmt), values)
    return group


# --- copy -------------------------------------------------------------------------------


@pytest.mark.parametrize("kind", KINDS)
def test_copy_is_plain_zarr_readable_by_xarray_zarr_tensorstore(
    spark_delta, drop, tmp_path, kind, monkeypatch
):
    monkeypatch.setattr(_export, "_TASK_BYTES", 1 << 16)  # several tasks, not one
    table, untouched = _scenario(spark_delta, drop, tmp_path, kind)
    want = _wants(spark_delta, table)
    if kind == "managed":  # the export still puts such a document right
        _leave_a_plain_fill(spark_delta, table)
    copy = tmp_path / "copy.zarr"

    assert gbx_zarr_export(spark_delta, table, str(copy)) == str(copy)

    fmt = 3 if kind == "managed" else 2
    group = _assert_readers(copy, want, fmt)
    for (
        key,
        raw,
    ) in untouched.items():  # bytes already in canonical form are not re-encoded
        assert (copy / key).read_bytes() == raw
    files = [p for p in copy.rglob("*") if p.is_file()]
    assert files and all(
        p.stat().st_size > 0 for p in files
    )  # a tombstone is no object
    assert not [p for p in files if ".tmp" in p.name]

    if kind == "managed":
        assert np.isnan(want["t2m"][12:24, 0:5, 0:6]).all()  # the tombstoned chunk
        # a float _FillValue attribute is stored in the form xarray decodes (Isaac M11);
        # the array's own fill_value is the spec's "NaN"
        assert group["t2m"].attrs["_FillValue"] == NAN_FILL
        assert np.isnan(group["t2m"].fill_value) and group["t2m"].dtype == np.float32
        doc = json.loads((copy / "t2m" / "zarr.json").read_text())
        assert doc["fill_value"] == "NaN"
        with xr.open_zarr(str(copy), chunks=None) as ds:
            assert np.isnan(ds.t2m.encoding["_FillValue"])
        root = json.loads((copy / "zarr.json").read_text())
        assert root["consolidated_metadata"]["metadata"]["t2m"]["shape"] == [48, 10, 12]
    else:
        assert not (copy / "t2m" / "0.0.0").exists()  # tombstone: absent, not empty
        assert np.isnan(want["t2m"][0:12, 0:37, 0:72]).all()
        # the vendor's .zmetadata said 48 hours; the copy's is made from the live documents
        zmeta = json.loads((copy / ".zmetadata").read_text())["metadata"]
        assert zmeta["t2m/.zarray"]["shape"][0] == 60 == group["t2m"].shape[0]


def test_copy_target_must_be_empty(spark_delta, drop, tmp_path, monkeypatch):
    table, _, _ = _managed(spark_delta, drop, tmp_path)
    want = _wants(spark_delta, table)

    full = tmp_path / "full"
    full.mkdir()
    (full / "keep.txt").write_text("mine")
    with pytest.raises(ValueError, match="not empty") as e:
        gbx_zarr_export(spark_delta, table, str(full))
    assert "Fix:" in str(e.value) and sorted(os.listdir(full)) == ["keep.txt"]
    afile = tmp_path / "afile"
    afile.write_text("x")
    with pytest.raises(ValueError, match="not a directory"):
        gbx_zarr_export(spark_delta, table, str(afile))

    empty = tmp_path / "empty"  # an existing empty directory is fine
    empty.mkdir()
    gbx_zarr_export(spark_delta, table, str(empty))
    _assert_readers(empty, want, 3)

    # a copy that fails midway leaves no half-written target behind
    def boom():
        raise RuntimeError("stop after the objects")

    monkeypatch.setattr(_export, "_AFTER_OBJECTS", boom)
    new, existing = tmp_path / "new", tmp_path / "existing"
    existing.mkdir()
    for target in (new, existing):
        with pytest.raises(RuntimeError, match="stop after"):
            gbx_zarr_export(spark_delta, table, str(target))
    assert not new.exists()
    assert existing.is_dir() and os.listdir(existing) == []

    # arguments are checked before any store is read
    for kwargs, match in [
        ({"to": None}, "needs a target"),
        ({"to": str(tmp_path / "x"), "method": "zip"}, "method must be"),
        ({"to": str(tmp_path / "x"), "access": "fuse"}, "access"),
        ({"to": str(tmp_path / "x"), "source": "/Volumes/c/s/v/store.zarr"}, "table"),
    ]:
        args = {"spark": None, "source": table, **kwargs}
        with pytest.raises(ValueError, match=match) as e:
            gbx_zarr_export(**args)
        assert "Fix:" in str(e.value)


# --- reference --------------------------------------------------------------------------


@pytest.mark.parametrize("kind", KINDS)
def test_reference_opens_in_xarray_via_fsspec(spark_delta, drop, tmp_path, kind):
    pytest.importorskip(
        "fsspec", reason="fsspec is the consumer's dependency; GeoBrix does not need it"
    )
    table, _ = _scenario(spark_delta, drop, tmp_path, kind)
    if kind == "managed":
        gbx_zarr_optimize(spark_delta, table)  # a reference names one region per shard
    want = _wants(spark_delta, table)
    fmt = 3 if kind == "managed" else 2
    out = tmp_path / "store.json"

    assert gbx_zarr_export(spark_delta, table, str(out), method="reference") == str(out)

    doc = json.loads(out.read_text())
    assert doc["version"] == 1
    refs = doc["refs"]
    assert isinstance(refs["zarr.json" if fmt == 3 else ".zgroup"], str)
    pointers = {k: v for k, v in refs.items() if isinstance(v, list)}
    assert pointers
    for path, offset, length in pointers.values():
        assert os.path.isabs(path) and "://" not in path and ":/" not in path
        assert os.path.isfile(path) and offset >= 0 and length > 0
    if kind == "registered_v2":
        assert "t2m/0.0.0" not in refs  # the tombstone: no reference at all
        assert "u10/0.0.0" in refs
    options = {"storage_options": {"fo": str(out)}, "zarr_format": fmt}
    with xr.open_zarr("reference://", consolidated=False, chunks=None, **options) as ds:
        for name, values in want.items():
            np.testing.assert_array_equal(ds[name].values, values)
    with xr.open_zarr("reference://", chunks=None, **options) as ds:  # consolidated
        np.testing.assert_array_equal(ds["t2m"].values, want["t2m"])


def test_reference_requires_optimized_shards(spark_delta, drop, tmp_path):
    table, _, _ = _managed(spark_delta, drop, tmp_path)  # chunk rows, no shard rows
    out = tmp_path / "refs.json"

    with pytest.raises(ZarrUnsupported) as e:
        gbx_zarr_export(spark_delta, table, str(out), method="reference")
    assert e.value.fix == 'run gbx_zarr_optimize first, or use method="copy"'
    assert not out.exists()

    gbx_zarr_optimize(spark_delta, table)
    gbx_zarr_export(spark_delta, table, str(out), method="reference")
    assert out.exists()

    gbx_zarr_write(  # a chunk row over a whole shard is not one region either
        _blocks(spark_delta, "t2m", (0, 0, 0), CHUNK, 7.0), table, mode="update"
    )
    with pytest.raises(ZarrUnsupported, match="t2m/c/0/0/0"):
        gbx_zarr_export(
            spark_delta, table, str(tmp_path / "again.json"), method="reference"
        )

    # JSON only (Ruling Z9), and no cloud location is recorded to point at (access="direct")
    with pytest.raises(ValueError, match="JSON") as bad:
        gbx_zarr_export(
            spark_delta, table, str(tmp_path / "r.parquet"), method="reference"
        )
    assert "Fix:" in str(bad.value)
    with pytest.raises(ZarrAccessUnavailable, match="direct") as direct:
        gbx_zarr_export(
            spark_delta,
            table,
            str(tmp_path / "d.json"),
            method="reference",
            access="direct",
        )
    assert direct.value.fix


# --- version ----------------------------------------------------------------------------


def test_export_pins_version(spark_delta, drop, tmp_path, monkeypatch):
    ds = _cube()
    table, root, _ = _managed(
        spark_delta, drop, tmp_path, ds.isel(time=slice(0, 24)), label="pin"
    )
    v1 = gbx_zarr_optimize(spark_delta, table)  # a reference needs whole shards
    gbx_zarr_tag(spark_delta, table, "first", version_as_of=v1)
    appended = uuid.uuid4().hex
    v2 = gbx_zarr_write(
        ds.isel(time=slice(24, 48)),
        table,
        mode="append",
        write_id=appended,
        spark=spark_delta,
    )
    v3 = gbx_zarr_optimize(spark_delta, table)
    assert v1 < v2 < v3
    first = H.read_variable(spark_delta, table, "t2m", v1)

    for pin, hours in ((v1, 24), ("tag:first", 24), (v2, 48), (v3, 48), (None, 48)):
        copy = tmp_path / f"copy_{hours}_{pin}".replace(":", "_")
        gbx_zarr_export(spark_delta, table, str(copy), version_as_of=pin)
        assert zarr.open_group(str(copy), mode="r")["t2m"].shape == (hours, 10, 12)
    ref = tmp_path / "v1.json"
    gbx_zarr_export(spark_delta, table, str(ref), method="reference", version_as_of=v1)
    options = {"storage_options": {"fo": str(ref)}, "zarr_format": 3}
    with xr.open_zarr(
        "reference://", consolidated=False, chunks=None, **options
    ) as ds1:
        np.testing.assert_array_equal(ds1.t2m.values, first)

    # a version vacuum has dropped is refused before any byte is written; a tagged one stays
    when = datetime.now(timezone.utc) + timedelta(days=30)
    monkeypatch.setattr(_maint, "_utcnow", lambda: when)
    gbx_zarr_vacuum(spark_delta, table)
    new, existing = tmp_path / "new", tmp_path / "existing"
    existing.mkdir()
    for target, method in (
        (new, "copy"),
        (existing, "copy"),
        (tmp_path / "r.json", "reference"),
    ):
        with pytest.raises(ZarrVersionUnavailable) as e:
            gbx_zarr_export(
                spark_delta, table, str(target), method=method, version_as_of=v1
            )
        assert e.value.fix
    assert not new.exists() and os.listdir(existing) == []
    assert not (tmp_path / "r.json").exists()
    kept = tmp_path / "kept"
    gbx_zarr_export(spark_delta, table, str(kept), version_as_of="tag:first")
    np.testing.assert_array_equal(
        zarr.open_group(str(kept), mode="r")["t2m"][...], first
    )

    # files that vanished behind GeoBrix's back: the same refusal, found by looking
    for pack in (root / "data").rglob("*.zpack"):
        if pack.parent.name == appended:
            pack.unlink()
    with pytest.raises(ZarrVersionUnavailable, match="no longer exist"):
        gbx_zarr_export(spark_delta, table, str(existing))
    assert os.listdir(existing) == []

    # publish writes the live overlay, so it takes no version; it is the sync module's publish
    seen = []
    monkeypatch.setattr(_export, "publish", lambda s, t: seen.append(t) or 41)
    assert gbx_zarr_export(spark_delta, table, method="publish") == 41
    assert seen == [table]
    with pytest.raises(ValueError, match="version_as_of"):
        gbx_zarr_export(spark_delta, table, method="publish", version_as_of=v1)
