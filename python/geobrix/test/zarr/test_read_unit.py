"""``gbx_zarr_read`` without a Spark session: planning and the task body, run in-process.

``test_read.py`` runs the read end to end on a local Spark session. The correctness matrix
lives here: ``_plan`` is the driver half, ``_task_function`` the half a Spark task runs,
and a plan's units are fed to it one at a time exactly as Spark would.
"""

import json
import logging

import numpy as np
import pyarrow as pa
import pytest

zarr = pytest.importorskip("zarr")

from pyspark import cloudpickle  # noqa: E402

from databricks.labs.gbx.zarr import _read  # noqa: E402
from databricks.labs.gbx.zarr.errors import ZarrUnsupported  # noqa: E402


def _plan(
    path, variables=None, sel=None, isel=None, output="blocks", decode=True, flag=None
):
    src = _read._open_source(None, str(path), "path", None, None)
    names = _read._choose_variables(variables, src)
    units, cfg = _read._plan(None, src, names, sel, isel, output, decode, flag)
    return src, names, units, cfg


def _run(units, cfg):
    """Every unit through the task body, as one Spark task each; the batches in order."""
    task = cloudpickle.loads(cloudpickle.dumps(_read._task_function(cfg)))
    out = []
    for i, unit in enumerate(units):
        batch = pa.record_batch(
            {"id": pa.array([i], pa.int64()), "unit": pa.array([unit.to_json()])}
        )
        out += list(task([batch]))
    return out


def _table(path, **kw):
    _, names, units, cfg = _plan(path, output="table", **kw)
    batches = _run(units, cfg)
    schema = pa.schema([(n, _read._TABLE_ARROW[k]) for n, k in cfg.columns])
    return cfg, (
        pa.Table.from_batches(batches, schema=schema)
        if batches
        else schema.empty_table()
    )


# --- plain stores with a corrupt array (per-array parse failures are tolerated) ----------


def _two_arrays(path, fmt):
    g = zarr.open_group(str(path), mode="w", zarr_format=fmt)
    for name, value in (("a", 1), ("b", 2)):
        kw = {"dimension_names": ("k",)} if fmt == 3 else {}
        arr = g.create_array(name, shape=(4,), chunks=(2,), dtype="f4", **kw)
        arr[:] = value
    if fmt == 2:  # the dimension names live in .zattrs
        for name in ("a", "b"):
            (path / name / ".zattrs").write_text(
                json.dumps({"_ARRAY_DIMENSIONS": ["k"]})
            )
    return path


def _corrupt(path, fmt, how):
    doc_path = path / "b" / ("zarr.json" if fmt == 3 else ".zarray")
    if how == "bad-json":
        doc_path.write_text("{not json")
        return
    doc = json.loads(doc_path.read_text())
    if how == "bad-shape":
        doc["shape"] = "x"
    else:
        key = "chunks" if fmt == 2 else "chunk_grid"
        doc[key] = (
            "zz"
            if fmt == 2
            else {"name": "regular", "configuration": {"chunk_shape": "zz"}}
        )
    doc_path.write_text(json.dumps(doc))


@pytest.mark.parametrize("fmt", [2, 3])
@pytest.mark.parametrize("how", ["bad-json", "bad-shape", "bad-chunks"])
def test_a_corrupt_array_does_not_stop_a_plain_read_of_its_neighbour(
    tmp_path, caplog, fmt, how
):
    path = _two_arrays(tmp_path / "s.zarr", fmt)
    _corrupt(path, fmt, how)
    with caplog.at_level(logging.WARNING, logger=_read.__name__):
        src, names, units, cfg = _plan(path)  # variables=None: the rest reads
    assert names == ["a"] and "b" in src.unsupported
    assert any("b" in r.getMessage() for r in caplog.records)
    _, names, units, cfg = _plan(path, variables=["a"])
    rows = [r for b in _run(units, cfg) for r in b.to_pylist()]
    assert [int(_read.block_to_numpy(r)[0]) for r in rows] == [1, 1]
    # asking for the corrupt one is a typed refusal that names it and says what to do
    with pytest.raises(ZarrUnsupported) as e:
        _plan(path, variables=["b"])
    assert "'b'" in str(e.value) and "Fix:" in str(e.value)


@pytest.mark.parametrize("fmt", [2, 3])
def test_a_corrupt_array_beside_a_group_still_walks_the_rest(tmp_path, fmt):
    path = tmp_path / "g.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=fmt)
    inner = g.create_group("fc")
    kw = {"dimension_names": ("k",)} if fmt == 3 else {}
    a = inner.create_array("a", shape=(4,), chunks=(2,), dtype="f4", **kw)
    a[:] = 3
    (path / "fc" / "a" / ("zarr.json" if fmt == 3 else ".zarray")).write_text("{oops")
    src = _read._open_source(None, str(path), "path", None, None)
    assert "fc/a" in src.unsupported and not src.arrays


# --- table columns keep the type of their values; missing data is NULL ----------------------

_KINDS = {
    "lev": "int",
    "x": "long",
    "i1": "byte",
    "i2": "short",
    "i4": "int",
    "i8": "long",
    "u1": "short",
    "u2": "int",
    "u4": "long",
    "u8": "decimal",
    "b": "boolean",
    "f2": "float",
    "f4": "float",
    "f8": "double",
    "m2": "short",  # a masked integer is still an integer, with NULLs
    "mf": "double",
    "pk": "double",  # packed: decoded to floating point
}
_SPARK = {
    "byte": "tinyint",
    "short": "smallint",
    "int": "int",
    "long": "bigint",
    "decimal": "decimal(20,0)",
    "boolean": "boolean",
    "float": "float",
    "double": "double",
    "timestamp": "timestamp",
}


def _same(got, want):
    assert len(got) == len(want)
    for g, w in zip(got, want):
        if isinstance(w, float) and w != w:
            assert isinstance(g, float) and g != g, (g, w)  # NaN data stays NaN
        elif isinstance(w, float):
            assert g == pytest.approx(w), (g, w)
        else:
            assert g == w and g is not None or w is None and g is None, (g, w)


def test_a_table_keeps_the_type_and_exact_value_of_every_column(dtype_matrix):
    path, want = dtype_matrix
    cfg, table = _table(path)
    assert dict(cfg.columns) == _KINDS
    assert [_read._spark_type(k).simpleString() for _, k in cfg.columns] == [
        _SPARK[_KINDS[n]] for n, _ in cfg.columns
    ]
    assert table.num_rows == 12
    for name, expected in want.items():
        _same(table.column(name).to_pylist(), expected)
    assert table.schema.field("u8").type == pa.decimal128(20, 0)
    assert table.column("u8")[0].as_py() == 2**64 - 1  # not rounded through float64
    assert table.column("i8")[0].as_py() == 2**53 + 1


def test_masked_cells_are_null_and_nan_data_is_not(dtype_matrix):
    path, want = dtype_matrix
    _, table = _table(path)
    assert table.column("m2").null_count == 3
    assert table.column("pk").null_count == 2
    mf = table.column("mf").to_pylist()
    assert (
        mf.count(None) == 2 and sum(v != v for v in mf if v is not None) == 2
    )  # 2 NaN data


def test_decode_false_gives_the_stored_values_and_nothing_is_null(dtype_matrix):
    path, _ = dtype_matrix
    cfg, table = _table(path, decode=False)
    assert dict(cfg.columns)["pk"] == "short" and dict(cfg.columns)["m2"] == "short"
    assert table.column("m2").null_count == 0 and table.column("m2")[1].as_py() == -1
    assert table.column("pk")[0].as_py() == 2


def test_a_complex_variable_is_refused_with_a_fix(tmp_path):
    g = zarr.open_group(str(tmp_path / "c.zarr"), mode="w", zarr_format=3)
    arr = g.create_array(
        "z", shape=(2,), chunks=(2,), dtype="complex64", dimension_names=("k",)
    )
    arr[:] = [1 + 2j, 3 - 1j]
    with pytest.raises(ZarrUnsupported) as e:
        _plan(tmp_path / "c.zarr", output="table")
    assert (
        "'z'" in str(e.value) and "complex" in str(e.value) and "Fix:" in str(e.value)
    )
    _plan(tmp_path / "c.zarr")  # blocks are untouched


def test_integer_zeros_survive_when_the_zarr_fill_value_is_not_a_mask(tmp_path):
    g = zarr.open_group(str(tmp_path / "z.zarr"), mode="w", zarr_format=2)
    a = g.create_array(
        "a",
        shape=(4,),
        chunks=(4,),
        dtype="i2",
        fill_value=0,
        attributes={"_ARRAY_DIMENSIONS": ["k"]},
    )
    a[:] = np.array([0, 3, 0, 5], dtype="i2")
    cfg, masked = _table(tmp_path / "z.zarr")  # xarray's default masks a v2 fill_value
    assert dict(cfg.columns)["a"] == "short"
    assert masked.column("a").to_pylist() == [None, 3, None, 5]
    cfg, kept = _table(tmp_path / "z.zarr", flag=False)
    assert kept.column("a").to_pylist() == [0, 3, 0, 5]


def test_an_integer_dimension_label_is_an_integer_column(dtype_matrix):
    path, _ = dtype_matrix
    cfg, table = _table(path, variables=["i2"])
    assert (
        cfg.columns[0] == ("lev", "int")
        and table.schema.field("lev").type == pa.int32()
    )
    assert sorted(set(table.column("lev").to_pylist())) == [10, 20, 30]


def test_a_masked_dimension_label_is_null(tmp_path):
    g = zarr.open_group(str(tmp_path / "l.zarr"), mode="w", zarr_format=3)
    lev = g.create_array(
        "lev",
        shape=(3,),
        chunks=(3,),
        dtype="int16",
        dimension_names=("lev",),
        attributes={"_FillValue": -1},
    )
    lev[:] = np.array([10, -1, 30], dtype="i2")
    v = g.create_array(
        "v", shape=(3,), chunks=(3,), dtype="f4", dimension_names=("lev",)
    )
    v[:] = 1
    cfg, table = _table(tmp_path / "l.zarr", variables=["v"], decode=True)
    assert dict(cfg.columns)["lev"] == "short"
    assert table.column("lev").to_pylist() == [10, None, 30]


# --- what a task is sent ----------------------------------------------------------------


def test_the_closure_does_not_grow_with_the_number_of_units(
    era5_v3_sharded, monkeypatch
):
    counts = []
    for budget in (10**12, 1):  # one unit, then one unit per chunk
        monkeypatch.setattr(_read, "_budget_bytes", lambda b=budget: b)
        _, _, units, cfg = _plan(era5_v3_sharded)
        blob = len(cloudpickle.dumps(_read._task_function(cfg)))
        counts.append((len(units), blob, sum(len(u.to_json()) for u in units)))
    (few, few_bytes, _), (many, many_bytes, units_bytes) = counts
    assert few == 2 and many == 32  # two variables, one unit each; then 16 chunks each
    assert abs(many_bytes - few_bytes) < 64  # the units are data, not closure
    assert units_bytes > 0


def test_a_task_carries_the_labels_of_the_selection_only(era5_v3_sharded):
    _, _, units, cfg = _plan(
        era5_v3_sharded, variables=["t2m"], isel={"time": slice(10, 20)}
    )
    span = cfg.coords["time"]
    assert (span.offset, len(span.values)) == (10, 10)
    assert (
        len(cfg.coords["latitude"].values) == 73
    )  # a dimension read whole ships whole
    rows = [r for b in _run(units, cfg) for r in b.to_pylist()]
    # hours 10..19 cut at the chunk edge (12): [10, 11] and [12, 19], per spatial chunk
    assert {r["coord_start"][0][11:13] for r in rows} == {"10", "12"}
    assert {r["coord_stop"][0][11:13] for r in rows} == {"11", "19"}


def test_the_work_is_one_row_per_unit_so_a_task_gets_its_own_unit(
    era5_v3_sharded, monkeypatch
):
    monkeypatch.setattr(_read, "_budget_bytes", lambda: 1)
    _, _, units, cfg = _plan(era5_v3_sharded, variables=["t2m"])
    assert len(units) == 16
    task = _read._task_function(cfg)
    one = pa.record_batch(
        {"id": pa.array([3], pa.int64()), "unit": [units[3].to_json()]}
    )
    rows = [r for b in task([one]) for r in b.to_pylist()]
    assert len(rows) == 1 and rows[0]["start"] == [
        c * n for c, n in zip(units[3].chunks[0], cfg.arrays["t2m"].chunks)
    ]


class _FakeManifest:
    """A stand-in for ``read_rows`` that records how it was asked."""

    _COLUMNS = ("kind", "array", "key", "shard_key", "file")

    def __init__(self):
        self.calls = []

    def __call__(
        self, spark, snap, *, kinds=None, arrays=None, shard_keys=None, keys=None
    ):
        self.calls.append({"shard_keys": shard_keys, "keys": keys, "arrays": arrays})
        asked = shard_keys if shard_keys is not None else keys
        rows = [
            {"kind": "shard", "array": arrays[0], "key": k, "shard_key": k, "file": "f"}
            for k in sorted(set(asked or ()))[:3]  # a few of the rows it was asked for
        ]
        return pa.table(
            {c: pa.array([r.get(c) for r in rows], pa.string()) for c in self._COLUMNS}
        )


def test_a_huge_selection_asks_the_manifest_for_exactly_its_keys(
    era5_v3_sharded, era5_v3_unsharded, monkeypatch
):
    from databricks.labs.gbx.zarr import _manifest
    from databricks.labs.gbx.zarr._testing import plain_store_rows

    fake = _FakeManifest()
    monkeypatch.setattr(_manifest, "read_rows", fake)
    sharded = plain_store_rows(era5_v3_sharded)[2]["t2m"]
    unsharded = plain_store_rows(era5_v3_unsharded)[2]["t2m"]
    keys = [f"c/{i}/0/0" for i in range(25_000)]  # far past any IN-list limit
    src = _read._Source(
        "table", "/r", None, {}, {}, {}, spark=None, table="t", snap=None
    )
    src.rows(sharded, keys)
    src.rows(unsharded, keys)
    assert fake.calls[0]["shard_keys"] == keys and fake.calls[0]["keys"] is None
    assert fake.calls[1]["keys"] == keys and fake.calls[1]["shard_keys"] is None
    src.rows(sharded, None)  # a whole small array (a coordinate) is the one full read
    assert fake.calls[2]["shard_keys"] is None and fake.calls[2]["keys"] is None
    assert (
        src.rows(sharded, []) == {} and len(fake.calls) == 3
    )  # nothing asked, nothing sent


def test_the_driver_coordinate_store_knows_the_unsupported_arrays(
    tmp_path, monkeypatch
):
    from databricks.labs.gbx.zarr import _manifest, _store

    path = tmp_path / "st.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    v = g.create_array("v", shape=(3,), chunks=(2,), dtype="f4", dimension_names=("k",))
    v[:] = 1
    k = g.create_array("k", shape=(3,), chunks=(2,), dtype="f4", dimension_names=("k",))
    k[:] = [1, 2, 3]
    s = g.create_array("station", shape=(3,), chunks=(2,), dtype=str)
    s[:] = np.array(["a", "bb", "c"], dtype=object)
    plain = _read._open_source(None, str(path), "path", None, None)
    seen = {}

    class Recording(_store.ManifestStore):
        def __init__(self, *args, **kwargs):
            seen.update(kwargs)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(_store, "ManifestStore", Recording)
    monkeypatch.setattr(_manifest, "read_rows", _all_rows(path, plain))
    src = _read._Source(
        "table",
        str(path),
        None,
        plain.metas,
        plain.arrays,
        plain.unsupported,
        table="t",
    )
    _read._read_coordinates(src, {"k"})
    assert set(seen["unsupported"]) == {"station"}


def _all_rows(path, plain):
    """``read_rows`` over the chunk files of a plain store (every array unsharded)."""

    def read(spark, snap, *, kinds=None, arrays=None, shard_keys=None, keys=None):
        rows = []
        for name in arrays:
            for f in sorted((path / name).rglob("*")):
                key = f.relative_to(path / name).as_posix()
                if f.is_file() and f.name != "zarr.json":
                    rows.append(
                        dict(
                            kind="chunk",
                            array=name,
                            key=key,
                            shard_key=None,
                            file=f"{name}/{key}",
                            offset=0,
                            length=f.stat().st_size,
                            origin="gbx",
                            identity=None,
                        )
                    )
        cols = (
            "kind",
            "array",
            "key",
            "shard_key",
            "file",
            "offset",
            "length",
            "origin",
            "identity",
        )
        return (
            pa.Table.from_pylist([{c: r.get(c) for c in cols} for r in rows])
            if rows
            else pa.table({c: pa.array([], pa.string()) for c in cols})
        )

    return read


# --- batches are cut by bytes as well as by rows ----------------------------------------------


def test_block_batches_are_cut_by_bytes(era5_v3_sharded, monkeypatch):
    import dataclasses

    monkeypatch.setattr(_read, "_budget_bytes", lambda: 10**12)
    _, _, units, cfg = _plan(era5_v3_sharded, variables=["t2m"])
    assert len(units) == 1 and len(units[0].chunks) == 16
    whole = _run(units, cfg)
    assert [b.num_rows for b in whole] == [16]  # rows cap: 64
    chunk = 12 * 37 * 72 * 4
    tight = dataclasses.replace(cfg, batch_bytes=2 * chunk)
    cut = _run(units, tight)
    assert sum(b.num_rows for b in cut) == 16 and len(cut) > 1
    assert (
        max(b.num_rows for b in cut) <= 3
    )  # 2 chunks reach the cap; edge chunks are smaller
    one = _run(units, dataclasses.replace(cfg, batch_bytes=1))
    assert [b.num_rows for b in one] == [
        1
    ] * 16  # a block over the cap is a batch of its own

    # the same cells either way
    def cells(batches):
        return sorted(
            (tuple(r["start"]), bytes(r["data"]))
            for b in batches
            for r in b.to_pylist()
        )

    assert cells(whole) == cells(cut) == cells(one)


def test_table_batches_are_cut_by_bytes(dtype_matrix):
    import dataclasses

    path, _ = dtype_matrix
    _, _, units, cfg = _plan(path, output="table")
    row_bytes = sum(_read._ARROW_WIDTH[k] for _, k in cfg.columns)
    tight = dataclasses.replace(cfg, batch_bytes=5 * row_bytes)
    batches = _run(units, tight)
    assert [b.num_rows for b in batches] == [5, 5, 2]
    assert sum(b.num_rows for b in _run(units, cfg)) == 12


def test_the_docstring_says_where_executor_errors_surface():
    doc = _read.gbx_zarr_read.__doc__
    assert "pyspark.errors.PythonException" in doc and "Fix:" in doc
    assert "keep their" in doc and "DECIMAL(20,0)" in doc and "NULL" in doc
