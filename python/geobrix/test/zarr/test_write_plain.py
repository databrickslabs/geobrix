"""``gbx_zarr_write(..., transactional=False)``: plain Zarr stores, no manifest.

A plain write has no table, no ownership ledger, no marker and no commit. What it keeps is
ordering (the metadata documents go last, so a crashed write is not a store) and the
write path's chunk, shard and codec choices. The Spark runs are the local session's; the
reads of what was written use zarr-python and xarray, the readers other tools would use.
"""

import os

import numpy as np
import pandas as pd
import pytest

zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

from test.zarr import _helpers as H  # noqa: E402

from databricks.labs.gbx.zarr import _write, _write_plain  # noqa: E402
from databricks.labs.gbx.zarr._register import OWNER_LEDGER, OWNER_MARKER  # noqa: E402
from databricks.labs.gbx.zarr._write import gbx_zarr_write  # noqa: E402
from databricks.labs.gbx.zarr.errors import ZarrError, ZarrGridMismatch  # noqa: E402

CHUNKS = {"time": 12, "latitude": 5, "longitude": 6}
DIMS = ("time", "latitude", "longitude")


def _dataset(hours=48, start="2023-01-01", ny=10, nx=12, seed=0):
    time = pd.date_range(start, periods=hours, freq="h")
    rng = np.random.default_rng(seed)
    shape = (hours, ny, nx)
    return xr.Dataset(
        {
            "t2m": (
                DIMS,
                (280 + rng.normal(size=shape)).astype("float32"),
                {"units": "K"},
            ),
            "u10": (DIMS, rng.normal(size=shape).astype("float32"), {"units": "m s-1"}),
        },
        coords={
            "time": ("time", time),
            "latitude": (
                "latitude",
                np.linspace(45.0, 36.0, ny),
                {"units": "degrees_north"},
            ),
            "longitude": ("longitude", np.arange(nx) * 1.0, {"units": "degrees_east"}),
        },
    )


@pytest.fixture
def no_hooks(monkeypatch):
    monkeypatch.setattr(_write, "_FAULT", None)
    monkeypatch.setattr(_write, "_BEFORE_COMMIT", None)
    return monkeypatch


def _write_plain_store(spark, ds, path, **kw):
    kw.setdefault("chunks", CHUNKS)
    return gbx_zarr_write(ds, str(path), transactional=False, spark=spark, **kw)


def _assert_plain(path):
    """Nothing of the transactional machinery is in the directory."""
    names = {p.name for p in path.rglob("*")}
    assert OWNER_MARKER not in names and OWNER_LEDGER not in names
    assert "_delta_log" not in names and "data" not in {p.name for p in path.iterdir()}
    assert not [n for n in names if n.endswith(".zpack")]


def _same(got: xr.Dataset, want: xr.Dataset, names=("t2m", "u10")):
    for n in names:
        np.testing.assert_array_equal(got[n].values, want[n].values)
    np.testing.assert_array_equal(got["time"].values, want["time"].values)
    np.testing.assert_array_equal(got["latitude"].values, want["latitude"].values)


def _read_variable(path, name):
    """The framework's own read of a plain store, in this process (``gbx_zarr_read``'s
    planner and task body), assembled."""
    from databricks.labs.gbx.zarr._read import block_to_numpy

    _, _, units, cfg = H.plan(str(path), variables=[name])
    rows = [r for b in H.run(units, cfg) for r in b.to_pylist()]
    stops = [r["stop"] for r in rows]
    shape = tuple(max(s[i] for s in stops) for i in range(len(stops[0])))
    first = block_to_numpy(rows[0])
    out = np.full(shape, np.nan if first.dtype.kind == "f" else 0, dtype=first.dtype)
    if first.dtype.kind == "M":
        out = np.full(shape, np.datetime64("NaT"), dtype=first.dtype)
    for r in rows:
        out[tuple(slice(a, b) for a, b in zip(r["start"], r["stop"]))] = block_to_numpy(
            r
        )
    return out


def _blocks_of(spark, ds, step=None):
    """A blocks DataFrame of ``ds``: data variables in blocks of ``step`` along time, and
    the coordinate arrays whole."""
    step = step or ds.sizes["time"]
    blocks = []
    for n in ("t2m", "u10"):
        v = ds[n].values
        for t in range(0, v.shape[0], step):
            blocks.append((n, (t, 0, 0), v[t : t + step]))
    blocks.append(("time", (0,), ds["time"].values.astype("datetime64[ns]")))
    blocks.append(("latitude", (0,), ds["latitude"].values))
    blocks.append(("longitude", (0,), ds["longitude"].values))
    return H.blocks_frame(spark, *blocks)


# --- create ----------------------------------------------------------------------------


def test_create_from_an_xarray_dataset_is_a_plain_store(
    spark_delta, tmp_path, no_hooks
):
    ds = _dataset()
    out = tmp_path / "out.zarr"
    result = _write_plain_store(spark_delta, ds, out)
    _assert_plain(out)
    assert result["path"] == str(out)
    assert result["chunks"] > 0 and result["bytes"] > 0
    assert set(result["arrays"]) >= {"t2m", "u10", "time", "latitude", "longitude"}
    _same(xr.open_zarr(out).load(), ds)
    root = zarr.open_group(str(out), mode="r")
    np.testing.assert_array_equal(root["t2m"][...], ds["t2m"].values)
    np.testing.assert_array_equal(_read_variable(out, "t2m"), ds["t2m"].values)


@pytest.mark.parametrize("step", [48, 12, 7])
def test_create_from_blocks_reads_back_equal(spark_delta, tmp_path, no_hooks, step):
    """Blocks on the shard grid (48 and 12 along time: whole or chunk-aligned) and across
    it (7) all land: the aligned ones are packed where they are made, the others through
    the shuffle."""
    ds = _dataset()
    out = tmp_path / "out.zarr"
    _write_plain_store(spark_delta, _blocks_of(spark_delta, ds, step), out)
    _assert_plain(out)
    _same(xr.open_zarr(out).load(), ds)
    np.testing.assert_array_equal(_read_variable(out, "u10"), ds["u10"].values)


def test_aligned_blocks_need_no_shuffle(spark_delta, tmp_path, no_hooks):
    def boom(cfg):
        raise AssertionError("the assemble stage (a shuffle) ran for aligned blocks")

    no_hooks.setattr(_write, "assemble_function", boom)
    ds = _dataset()
    out = tmp_path / "out.zarr"
    _write_plain_store(spark_delta, _blocks_of(spark_delta, ds, 48), out)
    _same(xr.open_zarr(out).load(), ds)


@pytest.mark.parametrize(
    "kw",
    [
        {"shards": False},
        {"shards": {"time": 24, "latitude": 5, "longitude": 6}},
        {
            "codecs": [
                {"name": "bytes", "configuration": {"endian": "little"}},
                {"name": "gzip", "configuration": {"level": 1}},
            ]
        },
    ],
)
def test_create_follows_the_chunk_shard_and_codec_choices(
    spark_delta, tmp_path, no_hooks, kw
):
    ds = _dataset()
    out = tmp_path / "out.zarr"
    _write_plain_store(spark_delta, ds, out, **kw)
    arr = zarr.open_group(str(out), mode="r")["t2m"]
    assert arr.chunks == (12, 5, 6)
    assert (arr.shards is None) == (kw.get("shards") is False)
    _same(xr.open_zarr(out).load(), ds)


def test_a_chunk_of_fill_stores_no_object(spark_delta, tmp_path, no_hooks):
    ds = _dataset()
    ds["t2m"][:12, :5, :6] = np.nan
    out = tmp_path / "out.zarr"
    _write_plain_store(spark_delta, ds, out, shards=False)
    assert not (out / "t2m" / "c" / "0" / "0" / "0").exists()
    assert (out / "t2m" / "c" / "1" / "0" / "0").exists()
    np.testing.assert_array_equal(xr.open_zarr(out)["t2m"].values, ds["t2m"].values)


def test_create_refuses_an_existing_store(spark_delta, tmp_path, no_hooks):
    out = tmp_path / "out.zarr"
    _write_plain_store(spark_delta, _dataset(), out)
    before = sorted(p.name for p in out.rglob("*"))
    with pytest.raises(ZarrError, match="Fix:"):
        _write_plain_store(spark_delta, _dataset(seed=1), out)
    assert sorted(p.name for p in out.rglob("*")) == before
    _same(xr.open_zarr(out).load(), _dataset())


@pytest.mark.parametrize("doc", ["zarr.json", ".zgroup", ".zarray"])
def test_create_refuses_any_store_document_at_the_path(
    spark_delta, tmp_path, no_hooks, doc
):
    out = tmp_path / "out.zarr"
    out.mkdir()
    (out / doc).write_text("{}")
    with pytest.raises(ZarrError, match="Fix:"):
        _write_plain_store(spark_delta, _dataset(), out)


# --- metadata last ---------------------------------------------------------------------


def test_metadata_documents_are_written_after_every_data_file(
    spark_delta, tmp_path, no_hooks
):
    """The fail-on-revert check for the ordering: when the documents are written, every
    data object is on disk and no zarr.json is; the root group's document goes last."""
    out = tmp_path / "out.zarr"
    seen = {}
    real = _write_plain.write_documents

    def spy(root, docs, *a, **kw):
        files = [p for p in out.rglob("*") if p.is_file()]
        seen["docs_before"] = [p for p in files if p.name in ("zarr.json", ".zgroup")]
        seen["data_before"] = [p for p in files if p.parent.name != out.name]
        return real(root, docs, *a, **kw)

    no_hooks.setattr(_write_plain, "write_documents", spy)
    ds = _dataset()
    _write_plain_store(spark_delta, ds, out)
    assert seen["docs_before"] == []
    assert len(seen["data_before"]) >= 5  # the objects of five arrays were there
    _same(xr.open_zarr(out).load(), ds)


def test_a_crash_after_the_data_files_leaves_no_store(spark_delta, tmp_path, no_hooks):
    out = tmp_path / "out.zarr"

    def die(*_a, **_k):
        raise RuntimeError("the driver died")

    no_hooks.setattr(_write, "_BEFORE_COMMIT", die)
    no_hooks.setattr(_write_plain, "discard", lambda *a, **k: None)  # a hard crash
    with pytest.raises(RuntimeError):
        _write_plain_store(spark_delta, _dataset(), out)
    assert any(p.is_file() for p in out.rglob("*"))  # the data files are there
    assert not list(out.rglob("zarr.json")) and not list(out.rglob(".zgroup"))
    with pytest.raises(Exception):
        xr.open_zarr(out)
    with pytest.raises(Exception):
        zarr.open_group(str(out), mode="r")


def test_a_failed_create_cleans_up_and_can_be_run_again(
    spark_delta, tmp_path, no_hooks
):
    out = tmp_path / "out.zarr"
    calls = {"n": 0}

    def once(*_a, **_k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("the driver died")

    no_hooks.setattr(_write, "_BEFORE_COMMIT", once)
    with pytest.raises(RuntimeError):
        _write_plain_store(spark_delta, _dataset(), out)
    assert not out.exists() or not any(out.iterdir())
    _write_plain_store(spark_delta, _dataset(), out)
    _same(xr.open_zarr(out).load(), _dataset())


# --- append ----------------------------------------------------------------------------


@pytest.mark.parametrize("as_blocks", [False, True])
def test_append_extends_time_and_reads_back_equal(
    spark_delta, tmp_path, no_hooks, as_blocks
):
    first, second = _dataset(48), _dataset(24, start="2023-01-03", seed=2)
    out = tmp_path / "out.zarr"
    _write_plain_store(spark_delta, first, out)
    new = _blocks_of(spark_delta, second, 24) if as_blocks else second
    result = gbx_zarr_write(
        new,
        str(out),
        transactional=False,
        mode="append",
        append_dim="time",
        spark=spark_delta,
    )
    _assert_plain(out)
    assert result["mode"] == "append"
    both = xr.concat([first, second], dim="time")
    got = xr.open_zarr(out).load()
    assert got.sizes["time"] == 72
    _same(got, both)


def test_append_into_a_partly_filled_chunk_keeps_the_old_cells(
    spark_delta, tmp_path, no_hooks
):
    """40 hours then 8 more: the chunk (and shard) holding hours 36-47 is half old, half
    new; the old half survives."""
    first, second = _dataset(40), _dataset(8, start="2023-01-02T16:00", seed=3)
    out = tmp_path / "out.zarr"
    _write_plain_store(spark_delta, first, out)
    gbx_zarr_write(
        second,
        str(out),
        transactional=False,
        mode="append",
        append_dim="time",
        spark=spark_delta,
    )
    _same(xr.open_zarr(out).load(), xr.concat([first, second], dim="time"))


def test_append_writes_the_documents_after_the_data(spark_delta, tmp_path, no_hooks):
    out = tmp_path / "out.zarr"
    first, second = _dataset(48), _dataset(24, start="2023-01-03", seed=2)
    _write_plain_store(spark_delta, first, out)
    seen = {}
    real = _write_plain.write_documents

    def spy(root, docs, *a, **kw):
        # the store still reads as it did: the old shape, until the documents are written
        seen["old_shape"] = zarr.open_group(str(out), mode="r")["t2m"].shape
        return real(root, docs, *a, **kw)

    no_hooks.setattr(_write_plain, "write_documents", spy)
    gbx_zarr_write(
        second, str(out), transactional=False, mode="append", spark=spark_delta
    )
    assert seen["old_shape"] == (48, 10, 12)
    assert zarr.open_group(str(out), mode="r")["t2m"].shape == (72, 10, 12)


def test_append_refuses_a_missing_store(spark_delta, tmp_path, no_hooks):
    with pytest.raises(ZarrError, match="Fix:"):
        gbx_zarr_write(
            _dataset(),
            str(tmp_path / "nope.zarr"),
            transactional=False,
            mode="append",
            spark=spark_delta,
        )


def test_append_refuses_mismatched_dims_and_dtype(spark_delta, tmp_path, no_hooks):
    out = tmp_path / "out.zarr"
    _write_plain_store(spark_delta, _dataset(48), out)
    later = _dataset(24, start="2023-01-03")
    wrong_dims = later.rename({"t2m": "t2m_x"}).drop_vars("u10")
    wrong_dims["t2m"] = (("time", "longitude", "latitude"), np.zeros((24, 12, 10)))
    with pytest.raises(ZarrGridMismatch, match="Fix:"):
        gbx_zarr_write(
            wrong_dims.drop_vars("t2m_x"),
            str(out),
            transactional=False,
            mode="append",
            spark=spark_delta,
        )
    # float values into an integer array would truncate them
    ints = xr.Dataset(
        {"cnt": (DIMS, np.ones((48, 10, 12), dtype="int32"))},
        coords={
            "time": _dataset(48).time,
            "latitude": _dataset().latitude,
            "longitude": _dataset().longitude,
        },
    )
    out2 = tmp_path / "ints.zarr"
    _write_plain_store(spark_delta, ints, out2)
    floats = xr.Dataset(
        {"cnt": (DIMS, np.full((24, 10, 12), 0.5, dtype="float64"))},
        coords={
            "time": _dataset(24, start="2023-01-03").time,
            "latitude": _dataset().latitude,
            "longitude": _dataset().longitude,
        },
    )
    with pytest.raises(ZarrError, match="Fix:"):
        gbx_zarr_write(
            floats, str(out2), transactional=False, mode="append", spark=spark_delta
        )
    assert zarr.open_group(str(out2), mode="r")["cnt"].shape == (48, 10, 12)


# --- refusals ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "target,kw",
    [
        ("main.default.some_table", {}),
        ("/tmp/x.zarr", {"mode": "update"}),
        ("/tmp/x.zarr", {"mode": "overwrite"}),
        ("/tmp/x.zarr", {"file_mode": "fuse"}),
        ("/tmp/x.zarr", {"retain": "1 day"}),
        ("/tmp/x.zarr", {"write_id": "a" * 32}),
        ("/tmp/x.zarr", {"optimize": True}),
        ("/tmp/x.zarr", {"root": "/tmp/y"}),
        ("/tmp/x.zarr", {"access": "direct"}),
    ],
)
def test_plain_refusals_name_a_fix(spark_delta, no_hooks, target, kw):
    with pytest.raises(ValueError, match="Fix:"):
        gbx_zarr_write(_dataset(), target, transactional=False, spark=spark_delta, **kw)


def test_transactional_is_still_the_default(spark_delta, tmp_path, no_hooks):
    """Without the opt-in the target is a manifest table and the root a separate argument."""
    with pytest.raises(ValueError, match="root"):
        gbx_zarr_write(_dataset(), str(tmp_path / "t"), spark=spark_delta)


def test_documents_of_arrays_are_written_together_and_the_root_group_last(
    tmp_path, monkeypatch
):
    """The arrays' documents may go in any order, but every one is down before the root's, and the document
    that makes the root a group is the last file written."""
    from databricks.labs.gbx.zarr import _export

    order = []
    real = _export._atomic_write

    def spy(path, parts, write_id, **kw):
        order.append(os.path.relpath(path, tmp_path).replace(os.sep, "/"))
        return real(path, parts, write_id, **kw)

    monkeypatch.setattr(_export, "_atomic_write", spy)
    docs = {f"v{i}/zarr.json": b"{}" for i in range(6)}
    docs.update({"zarr.json": b"{}", "zarr.metadata.json": b"{}"})
    _export._write_documents(str(tmp_path), docs, "w1")
    assert sorted(order[:6]) == sorted(k for k in docs if "/" in k)
    assert order[-2:] == ["zarr.metadata.json", "zarr.json"]
    assert all((tmp_path / k).read_bytes() == b"{}" for k in docs)
    assert not [p for p in tmp_path.rglob(".*.tmp")]
