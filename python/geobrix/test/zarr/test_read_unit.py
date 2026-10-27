"""``gbx_zarr_read`` without a Spark session: planning and the task body, run in-process.

``test_read.py`` runs the read end to end on a local Spark session, once per behaviour. The
correctness matrix lives here: ``_plan`` is the driver half, ``_task_function`` the half a
Spark task runs, and a plan's units are fed to it one at a time exactly as Spark would.
Selections, layouts, decoding and the argument checks run here against zarr's and xarray's
own reading of the same store; ``_helpers.registered_source`` stands in for a table's rows.
"""

import json
import logging

import numpy as np
import pyarrow as pa
import pytest

zarr = pytest.importorskip("zarr")

from test.zarr import _helpers as H  # noqa: E402

from pyspark import cloudpickle  # noqa: E402

from databricks.labs.gbx.zarr import _read  # noqa: E402
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrAccessUnavailable,
    ZarrError,
    ZarrGridMismatch,
    ZarrUnsupported,
    ZarrVersionUnavailable,
)


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
    "u8m": "decimal",  # a masked uint64 is still a DECIMAL(20,0), with NULLs
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
    assert table.column("u8m").null_count == 2
    assert (
        table.column("u8m")[1].as_py() == 2**63 + 1
    )  # exact, beyond int64 and float64
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


# --- table, blocks and xarray agree (xarray's _Unsigned and dtype: bool coders) -------------


def _one_array(tmp_path, fmt, values, dtype, attrs, name="v"):
    """A one-array store with dimension ``k`` (no coordinate), v2 or v3."""
    path = (
        tmp_path / f"a{fmt}_{abs(hash((name, json.dumps(attrs, sort_keys=True))))}.zarr"
    )
    g = zarr.open_group(str(path), mode="w", zarr_format=fmt)
    attributes, kw = dict(attrs), {}
    if fmt == 2:
        attributes["_ARRAY_DIMENSIONS"] = ["k"]
    else:
        kw["dimension_names"] = ("k",)
    arr = g.create_array(
        name,
        shape=(len(values),),
        chunks=(len(values),),
        dtype=dtype,
        fill_value=None if fmt == 2 else 0,
        attributes=attributes,
        **kw,
    )
    arr[:] = np.asarray(values, dtype=dtype)
    return path


def _three_ways(path, name="v"):
    """``(table column as a list, blocks array, xarray array)`` of one variable."""
    import xarray as xr
    from xarray.backends.zarr import ZarrStore

    _, table = _table(path, variables=[name])
    _, _, units, cfg = _plan(path, variables=[name])
    rows = [r for b in _run(units, cfg) for r in b.to_pylist()]
    blocks = np.concatenate([_read.block_to_numpy(r) for r in rows])
    theirs = xr.open_dataset(
        ZarrStore.open_group(str(path), mode="r", consolidated=False)
    )
    return table.column(name).to_pylist(), blocks, theirs[name].values


def _agree(column, blocks, theirs):
    """Missing cells are NULL in the table and NaN/NaT in blocks and xarray; the rest equal."""
    assert len(column) == len(blocks) == len(theirs)
    for got, b, x in zip(column, blocks, theirs):
        gone = got is None
        assert gone == bool(np.isnan(b) if b.dtype.kind in "fM" else False)
        assert gone == bool(np.isnan(x) if x.dtype.kind in "fM" else False)
        if not gone:
            assert got == b.item() == x.item() or (got == int(b) == int(x))


@pytest.mark.parametrize("fmt", [2, 3])
def test_table_blocks_and_xarray_agree_on_an_unsigned_array(tmp_path, fmt):
    path = _one_array(tmp_path, fmt, [-1, -56, 0, 100], "i1", {"_Unsigned": "true"})
    cfg, _ = _table(path)
    assert dict(cfg.columns)["v"] == "short"  # int8 -> uint8 -> SMALLINT
    column, blocks, theirs = _three_ways(path)
    assert column == [255, 200, 0, 100] and list(blocks) == [255, 200, 0, 100]
    assert blocks.dtype == np.dtype("u1") and list(theirs) == [255, 200, 0, 100]


@pytest.mark.parametrize("fmt", [2, 3])
def test_table_blocks_and_xarray_agree_on_unsigned_false(tmp_path, fmt):
    path = _one_array(tmp_path, fmt, [65535, 5, 7], "u2", {"_Unsigned": "false"})
    cfg, _ = _table(path)
    assert dict(cfg.columns)["v"] == "short"  # uint16 -> int16 -> SMALLINT
    column, blocks, theirs = _three_ways(path)
    assert (
        column == [-1, 5, 7]
        and list(blocks) == [-1, 5, 7]
        and list(theirs) == [-1, 5, 7]
    )


@pytest.mark.parametrize("fmt", [2, 3])
def test_table_blocks_and_xarray_agree_on_a_bool_dtype_attribute(tmp_path, fmt):
    path = _one_array(tmp_path, fmt, [0, 1, 1, 0], "i1", {"dtype": "bool"})
    cfg, _ = _table(path)
    assert dict(cfg.columns)["v"] == "boolean"
    column, blocks, theirs = _three_ways(path)
    want = [False, True, True, False]
    assert column == want and list(blocks) == want and list(theirs) == want


@pytest.mark.parametrize("fmt", [2, 3])
def test_table_blocks_and_xarray_agree_on_an_unsigned_array_with_a_fill(tmp_path, fmt):
    attrs = {"_Unsigned": "true", "_FillValue": -1}
    path = _one_array(tmp_path, fmt, [-1, -56, 0, 100], "i1", attrs)
    cfg, _ = _table(path)
    assert dict(cfg.columns)["v"] == "short"  # still an integer column, with a NULL
    column, blocks, theirs = _three_ways(path)
    assert column == [None, 200, 0, 100]  # the stored -1 is 255 unsigned: the fill
    assert np.isnan(blocks[0]) and list(blocks[1:]) == [200, 0, 100]
    assert np.isnan(theirs[0]) and list(theirs[1:]) == [200, 0, 100]
    _agree(column, blocks, theirs)


@pytest.mark.parametrize("fmt", [2, 3])
def test_table_blocks_and_xarray_agree_on_an_unsigned_time_coordinate(tmp_path, fmt):
    attrs = {
        "units": "hours since 2000-01-01",
        "calendar": "standard",
        "_Unsigned": "true",
    }
    g_path = tmp_path / f"t{fmt}.zarr"
    g = zarr.open_group(str(g_path), mode="w", zarr_format=fmt)
    kw = {"dimension_names": ("time",)} if fmt == 3 else {}
    a = dict(attrs, **({"_ARRAY_DIMENSIONS": ["time"]} if fmt == 2 else {}))
    t = g.create_array(
        "time",
        shape=(2,),
        chunks=(2,),
        dtype="i2",
        fill_value=None if fmt == 2 else 0,
        attributes=a,
        **kw,
    )
    t[:] = np.array([-1, 10], dtype="i2")
    v = g.create_array(
        "v",
        shape=(2,),
        chunks=(2,),
        dtype="f4",
        attributes={"_ARRAY_DIMENSIONS": ["time"]} if fmt == 2 else {},
        **kw,
    )
    v[:] = 1
    cfg, table = _table(g_path, variables=["v"])
    got = [x.replace(tzinfo=None) for x in table.column("time").to_pylist()]
    want = [np.datetime64("2007-06-23T15:00"), np.datetime64("2000-01-01T10:00")]
    assert [np.datetime64(x, "m") for x in got] == want
    # the labels blocks carry are the same times
    _, _, units, bcfg = _plan(g_path, variables=["v"])
    row = _run(units, bcfg)[0].to_pylist()[0]
    assert row["coord_start"][0].startswith("2007-06-23T15:00")
    assert row["coord_stop"][0].startswith("2000-01-01T10:00")


# --- Task 9 follow-ups: planning branches and docstrings ---------------------------------


def _group(path, **arrays):
    """A v3 store: ``name=(shape, dims, dtype, values)`` per array."""
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    for name, (shape, dims, dtype, values) in arrays.items():
        arr = g.create_array(
            name, shape=shape, chunks=shape, dtype=dtype, dimension_names=dims
        )
        if values is not None:
            arr[...] = values
    return path


def test_row_unit_key_takes_a_missing_shard_key_from_the_key(
    era5_v3_sharded, era5_v3_unsharded
):
    from databricks.labs.gbx.zarr._plan import row_unit_key, unit_key
    from databricks.labs.gbx.zarr._testing import plain_store_rows

    sharded = plain_store_rows(era5_v3_sharded)[2]["t2m"]
    plain = plain_store_rows(era5_v3_unsharded)[2]["t2m"]
    chunk = {"kind": "chunk", "key": "c/3/1/1", "shard_key": None}
    # an unsharded array: the row is its own unit
    assert row_unit_key(plain, chunk) == "c/3/1/1"
    # a sharded one: the shard_key the row says, else a shard row's own key, else the
    # shard the chunk's coordinates lie in
    assert row_unit_key(sharded, {**chunk, "shard_key": "c/1/0/0"}) == "c/1/0/0"
    assert row_unit_key(sharded, {"kind": "shard", "key": "c/1/0/0"}) == "c/1/0/0"
    assert row_unit_key(sharded, chunk) == "c/1/0/0" == unit_key(sharded, (3, 1, 1))


def test_parse_documents_names_each_malformed_document_and_keeps_the_rest(
    era5_v3_unsharded, era5_v2
):
    good = (era5_v3_unsharded / "t2m" / "zarr.json").read_bytes()
    no_config = json.loads(good)
    no_config["chunk_grid"] = {"name": "regular"}  # a KeyError, not a refusal
    v2_good = (era5_v2 / "t2m" / ".zarray").read_bytes()
    metas = {
        "zarr.json": b'{"zarr_format": 3, "node_type": "group"}',
        "ok/zarr.json": good,
        "keys/zarr.json": json.dumps(no_config).encode(),
        "json/zarr.json": b"{not json",
        "list/zarr.json": b"[]",
        "v2ok/.zarray": v2_good,
        "v2keys/.zarray": b'{"shape": [2], "chunks": [2]}',
        "v2json/.zarray": b"{",
    }
    arrays, unsupported = _read.parse_documents(metas)
    assert sorted(arrays) == ["ok", "v2ok"]
    assert sorted(unsupported) == ["json", "keys", "list", "v2json", "v2keys"]
    for path, kind in (
        ("keys", "KeyError"),
        ("json", "JSONDecodeError"),
        ("list", "AttributeError"),
        ("v2keys", "KeyError"),
        ("v2json", "JSONDecodeError"),
    ):
        assert "malformed" in unsupported[path] and kind in unsupported[path], path


def test_parse_documents_keeps_a_corrupt_group_out_of_the_arrays():
    """A group whose document is unreadable is not an array, and what is under it is
    refused for that cause."""
    leaf = (
        b'{"zarr_format": 3, "node_type": "array", "shape": [2], "data_type": "float32",'
        b' "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [2]}},'
        b' "chunk_key_encoding": {"name": "default"}, "fill_value": 0, "codecs": []}'
    )
    metas = {
        "zarr.json": b'{"zarr_format": 3, "node_type": "group"}',
        "fine/zarr.json": b'{"zarr_format": 3, "node_type": "group"}',
        "fine/x/zarr.json": leaf,
        "grp/zarr.json": b"{oops",
        "grp/x/zarr.json": leaf,
        "grp/sub/zarr.json": b'{"zarr_format": 3, "node_type": "group"}',
        "grp/sub/y/zarr.json": leaf,
        "lone/zarr.json": b"{oops",  # no node under it: an array nobody can decode
    }
    arrays, unsupported = _read.parse_documents(metas)
    assert sorted(arrays) == ["fine/x"]
    assert sorted(unsupported) == ["grp/sub/y", "grp/x", "lone"]
    for path in ("grp/x", "grp/sub/y"):
        assert "group document unreadable" in unsupported[path]
        assert "'grp'" in unsupported[path], unsupported[path]
    assert "malformed" in unsupported["lone"]


def test_an_undecodable_array_is_a_coordinate_only_if_it_is_one_dimensional(tmp_path):
    names = np.array(["a", "bb"], dtype=object)
    # a string array of the dimension's own name and dims: labels that cannot be read
    one_d = _group(
        tmp_path / "one.zarr",
        v=((2,), ("x",), "f4", 1),
        x=((2,), ("x",), str, names),
    )
    with pytest.raises(ZarrUnsupported, match="no labels for 'x'"):
        _plan(one_d, output="table")
    # the same name on a 2-D array is not a coordinate of x: x is labelled by position
    two_d = _group(
        tmp_path / "two.zarr",
        v=((2, 2), ("p", "x"), "f4", 1),
        x=((2, 2), ("p", "q"), str, np.array([["a", "b"], ["c", "d"]], dtype=object)),
    )
    cfg, table = _table(two_d, variables=["v"])
    assert dict(cfg.columns) == {"p": "long", "x": "long", "v": "float"}
    assert sorted(table.column("x").to_pylist()) == [0, 0, 1, 1]


def test_a_store_with_no_gridded_variable_asks_for_variables(tmp_path):
    path = _group(
        tmp_path / "bare.zarr",
        k=((3,), ("k",), "f4", [1, 2, 3]),  # a coordinate
        crs=((), (), "i4", 4326),  # a scalar
    )
    with pytest.raises(ValueError, match="no gridded variable") as e:
        _plan(path)
    assert "variables=" in str(e.value) and "Fix:" in str(e.value)


def test_a_table_cannot_hold_a_coordinate_and_a_dimension_of_the_same_name(tmp_path):
    path = _group(
        tmp_path / "same.zarr",
        k=((3,), ("k",), "f4", [1, 2, 3]),
        v=((3,), ("k",), "f4", 1),
    )
    with pytest.raises(ValueError, match="name of a dimension") as e:
        _plan(path, variables=["k", "v"], output="table")
    assert "'k'" in str(e.value) and "Fix:" in str(e.value)
    _plan(path, variables=["k", "v"])  # as blocks both read


def test_a_coordinate_with_the_wrong_number_of_labels_is_refused(tmp_path):
    path = _group(
        tmp_path / "short.zarr",
        k=((3,), ("k",), "f4", [1, 2, 3]),
        v=((4,), ("k",), "f4", 1),
    )
    with pytest.raises(ZarrError, match="3 labels but 'v' has 4 cells") as e:
        _plan(path, variables=["v"])
    assert "'k'" in str(e.value) and "Fix:" in str(e.value)


def test_a_time_column_exists_only_where_the_values_are_times(tmp_path):
    """The table's column kind comes from the values' dtype, so a TIMESTAMP column never
    holds numbers: with decode=False a CF time coordinate is a plain integer column."""
    g = zarr.open_group(str(tmp_path / "t.zarr"), mode="w", zarr_format=3)
    t = g.create_array(
        "time",
        shape=(2,),
        chunks=(2,),
        dtype="i8",
        dimension_names=("time",),
        attributes={"units": "hours since 2000-01-01"},
    )
    t[:] = [0, 6]
    v = g.create_array(
        "v", shape=(2,), chunks=(2,), dtype="f4", dimension_names=("time",)
    )
    v[:] = 1
    cfg, decoded = _table(tmp_path / "t.zarr", variables=["v"])
    assert dict(cfg.columns)["time"] == "timestamp"
    cfg, raw = _table(tmp_path / "t.zarr", variables=["v"], decode=False)
    assert dict(cfg.columns)["time"] == "long"
    assert raw.column("time").to_pylist() == [0, 6]


@pytest.mark.parametrize("how", ["v3 _FillValue", "v2 fill_value", "v2 _FillValue"])
def test_a_bool_dtype_array_whose_fill_is_a_stored_value_is_null_in_a_table(
    tmp_path, how
):
    """Ruling Z36. [0, 1, 1, 0] read as booleans with a stored 0 as an explicit fill: the
    table masks on the integers, before the cast, so those cells are NULL. Blocks (and
    xarray) cast the masked cell's NaN to True. Ruling Z38(2): the Zarr v2 fill_value alone
    is not a fill for such an array (its 0 is the stored False), so nothing is masked;
    xarray does mask it."""
    fmt = 3 if how.startswith("v3") else 2
    attrs = {"dtype": "bool"}
    if how.endswith("_FillValue"):
        attrs["_FillValue"] = 0
    path = _one_array(tmp_path, fmt, [0, 1, 1, 0], "i1", attrs)
    if how == "v2 fill_value":  # the Zarr fill_value, which xarray masks for v2
        doc = path / "v" / ".zarray"
        zarray = json.loads(doc.read_text())
        zarray["fill_value"] = 0
        doc.write_text(json.dumps(zarray))
    column, blocks, theirs = _three_ways(path)
    if how == "v2 fill_value":
        assert column == [False, True, True, False]
        assert list(blocks) == [False, True, True, False]
    else:
        assert column == [None, True, True, None]
        assert list(blocks) == [True, True, True, True]
    assert list(theirs) == [True, True, True, True]
    cfg, _ = _table(path)
    assert dict(cfg.columns)["v"] == "boolean"  # still a boolean column, with NULLs


def test_the_work_frame_docstring_says_units_are_broadcast_once_per_executor():
    doc = " ".join(_read._work_frame.__doc__.split())
    assert "once per executor" in doc and "not once per task" in doc
    assert (
        "not all of them" not in doc
    )  # the claim that a task is sent only its own unit
    assert "sent only its own" not in " ".join(_read._TaskConfig.__doc__.split())


@pytest.mark.parametrize(
    "attrs, table_values, block_values",
    [
        # zarr's default v2 fill_value 0 is the stored False: it masks nothing
        ({"dtype": "bool"}, [False, True, True, False], [False, True, True, False]),
        # an explicit _FillValue still masks (NULL in a table; xarray's True in blocks)
        (
            {"dtype": "bool", "_FillValue": 0},
            [None, True, True, None],
            [True, True, True, True],
        ),
    ],
)
def test_a_v2_bool_dtype_array_masks_only_on_an_explicit_fill(
    tmp_path, attrs, table_values, block_values
):
    """Carry Z38(2): an integer v2 array read as booleans never masks on its fill_value."""
    path = tmp_path / "flags.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=2)
    arr = g.create_array(
        "v",
        shape=(4,),
        chunks=(4,),
        dtype="i1",
        fill_value=0,
        attributes={"_ARRAY_DIMENSIONS": ["k"], **attrs},
    )
    arr[:] = [0, 1, 1, 0]
    _, table = _table(path, variables=["v"])
    assert table.column("v").to_pylist() == table_values
    _, _, units, cfg = _plan(path, variables=["v"])
    rows = [r for b in _run(units, cfg) for r in b.to_pylist()]
    blocks = np.concatenate([_read.block_to_numpy(r) for r in rows])
    assert blocks.dtype == np.dtype("bool") and blocks.tolist() == block_values


# --- the read matrix: selections and layouts, checked against zarr and xarray --------------
#
# Each case ran as a Spark job in test_read.py. The expected cells are written out here, by
# hand, from the axes of the ERA5-like cube (48 hours; 90..-90 by 2.5; 0..357.5 by 2.5), so
# the planner is checked against the selection, not against itself.

SHAPE = (48, 73, 144)
_SEAM = [*range(140, 144), *range(0, 5)]

# name: (label selection, position selection, the cells it must give: time, lat, lon)
CASES = {
    "a label box across the seam": (
        {"latitude": slice(60, 50), "longitude": slice(-10, 10)},
        {"time": slice(0, 30)},
        (range(30), range(12, 17), _SEAM),
    ),
    "the south edge and the seam the other way": (
        {"latitude": slice(-80, -90), "longitude": slice(350, 5)},
        {"time": slice(40, 48)},
        (range(40, 48), range(68, 73), [*range(140, 144), 0, 1, 2]),
    ),
    "times by label, the rest by position": (
        {"time": slice("2023-01-01T05", "2023-01-02T03")},
        {"latitude": [0, 36, 37, 72], "longitude": slice(140, 144)},
        (range(5, 28), [0, 36, 37, 72], range(140, 144)),
    ),
    "a scalar label keeps its dimension": (
        {"longitude": 357.5, "latitude": -90.0},
        None,
        (range(48), [72], [143]),
    ),
    "everything": (None, None, (range(48), range(73), range(144))),
}
LAYOUTS = ["era5_v3_sharded", "era5_v3_unsharded", "era5_v2"]


def _assert_blocks_are_the_selection(rows, store, want, expected=None):
    assert {r["variable"] for r in rows} == {"t2m", "u10"}  # not the coordinates
    cells = H.mask(SHAPE, *want)
    for name in ("t2m", "u10"):
        got, seen = H.assemble(rows, name, SHAPE)
        np.testing.assert_array_equal(seen, cells)  # exactly the selection, no overlap
        truth = expected if (name == "t2m" and expected is not None) else None
        truth = H.direct(store, name) if truth is None else truth
        np.testing.assert_array_equal(got[cells], truth[cells])


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("case", list(CASES))
def test_blocks_cover_exactly_the_selection_in_every_layout(request, layout, case):
    store = request.getfixturevalue(layout)
    sel, isel, want = CASES[case]
    rows = H.read_blocks(store, sel=sel, isel=isel)
    _assert_blocks_are_the_selection(rows, store, want)


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("case", list(CASES))
def test_a_registered_store_reads_like_the_files_it_registers(
    request, monkeypatch, tmp_path, layout, case
):
    store = request.getfixturevalue(layout)
    sel, isel, want = CASES[case]
    src = H.registered_source(monkeypatch, store, overlay=tmp_path)
    _assert_blocks_are_the_selection(
        H.read_blocks(src, sel=sel, isel=isel), store, want
    )


@pytest.mark.parametrize("case", ["a label box across the seam", "everything"])
def test_an_inner_chunk_rewritten_in_the_overlay_replaces_the_one_in_its_shard(
    era5_v3_sharded, monkeypatch, tmp_path, case
):
    from databricks.labs.gbx.zarr._testing import (
        plain_store_rows,
        rewrite_one_inner_chunk,
    )

    _, rows, arrays = plain_store_rows(era5_v3_sharded, origin="external")
    rows, expected = rewrite_one_inner_chunk(
        rows, arrays["t2m"], tmp_path, chunk=(1, 0, 1), value=7.0
    )
    src = H.registered_source(monkeypatch, era5_v3_sharded, overlay=tmp_path, rows=rows)
    sel, isel, want = CASES[case]
    got = H.read_blocks(src, sel=sel, isel=isel)
    _assert_blocks_are_the_selection(got, era5_v3_sharded, want, expected=expected)
    if case == "everything":
        assert (
            expected[12:24, :37, 72:] == 7.0
        ).all()  # the rewrite is in what was read


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("case", [c for c in CASES if c != "everything"])
def test_a_table_read_equals_xarray_in_every_layout(request, layout, case):
    xr = pytest.importorskip("xarray")
    store = request.getfixturevalue(layout)
    sel, isel, want = CASES[case]
    cfg, table = H.read_table(store, sel=sel, isel=isel)
    got = table.to_pandas()
    got["time"] = got["time"].dt.tz_localize(None).astype("datetime64[ns]")
    theirs = (
        xr.open_zarr(str(store), consolidated=False)
        .isel(time=list(want[0]), latitude=list(want[1]), longitude=list(want[2]))
        .to_dataframe()
        .reset_index()
    )
    assert len(got) == len(theirs) == int(np.prod([len(list(w)) for w in want]))
    both = got.merge(
        theirs,
        on=["time", "latitude", "longitude"],
        suffixes=("", "_xr"),
        how="outer",
        indicator=True,
    )
    assert (both["_merge"] == "both").all()  # the same cells, neither more nor fewer
    for name in ("t2m", "u10"):
        np.testing.assert_array_equal(both[name].values, both[name + "_xr"].values)


def test_a_file_uri_reads_like_the_path(era5_v3_sharded):
    a = H.read_blocks(f"file:{era5_v3_sharded}", variables=["u10"])
    b = H.read_blocks(str(era5_v3_sharded), variables=["u10"])
    ga, _ = H.assemble(a, "u10", SHAPE)
    gb, _ = H.assemble(b, "u10", SHAPE)
    np.testing.assert_array_equal(ga, gb)
    np.testing.assert_array_equal(ga, H.direct(era5_v3_sharded, "u10"))


def test_block_helpers_round_trip_a_row(era5_v3_sharded):
    (row,) = H.read_blocks(
        era5_v3_sharded,
        variables="t2m",
        isel={"time": slice(0, 4), "latitude": slice(0, 3), "longitude": slice(5, 9)},
    )
    arr = _read.block_to_numpy(row)
    np.testing.assert_array_equal(arr, H.direct(era5_v3_sharded, "t2m")[0:4, 0:3, 5:9])
    assert arr.flags.writeable
    xa = _read.block_to_xarray(row)
    assert xa.dims == ("time", "latitude", "longitude") and xa.name == "t2m"
    assert xa.attrs["units"] == "K"
    np.testing.assert_array_equal(xa.values, arr)


def test_blocks_carry_their_labels_and_the_attributes_decoding_left(era5_v3_sharded):
    sel, isel, _ = CASES["a label box across the seam"]
    rows = H.read_blocks(era5_v3_sharded, sel=sel, isel=isel)
    first = next(r for r in rows if r["variable"] == "t2m" and r["start"][2] == 0)
    assert first["dims"] == ["time", "latitude", "longitude"]
    assert first["coord_start"][1] == "60.0" and first["coord_stop"][2] == "10.0"
    assert first["coord_start"][0].startswith("2023-01-01T00")
    assert first["dtype"] == "<f4" and first["fill_value"] == "NaN"
    attrs = H.json_attrs(first)
    assert attrs["units"] == "K" and "_FillValue" not in attrs  # applied by decoding


# --- axes and variables that are not the ERA5 cube -------------------------------------------


def test_blocks_on_an_irregular_axis(tmp_path):
    from databricks.labs.gbx.zarr._testing import make_era5_like

    # Gaussian-like: monotonic, no constant step
    lat = 90 * np.tanh(np.linspace(2.5, -2.5, 73)) / np.tanh(2.5)
    assert np.ptp(np.diff(lat)) > 1.0
    path = make_era5_like(tmp_path / "gauss.zarr", latitude=lat)
    rows = H.read_blocks(
        path,
        variables=["t2m"],
        sel={"latitude": slice(lat[20], lat[40])},
        isel={"time": slice(0, 12)},
    )
    got, seen = H.assemble(rows, "t2m", SHAPE)
    want = H.mask(SHAPE, range(12), range(20, 41), range(144))
    np.testing.assert_array_equal(seen, want)
    np.testing.assert_array_equal(got[want], H.direct(path, "t2m")[want])


def test_a_dimension_the_attributes_call_a_longitude_wraps_at_the_seam(tmp_path):
    """coord_attrs reach the selection planner: the name 'xi' says nothing, the units do."""
    path = tmp_path / "xi.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    xi = g.create_array(
        "xi", shape=(144,), chunks=(72,), dtype="f8", dimension_names=("xi",)
    )
    xi[:] = np.arange(144) * 2.5
    xi.attrs["units"] = "degrees_east"
    v = g.create_array(
        "v", shape=(144,), chunks=(72,), dtype="f4", dimension_names=("xi",)
    )
    v[:] = np.arange(144, dtype="f4")
    rows = H.read_blocks(path, sel={"xi": slice(-10, 10)})
    _, seen = H.assemble(rows, "v", (144,))
    np.testing.assert_array_equal(
        np.flatnonzero(seen), [*range(0, 5), *range(140, 144)]
    )


def test_a_scalar_variable_reads_as_one_block(tmp_path):
    path = tmp_path / "scalar.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    crs = g.create_array("crs", shape=(), chunks=(), dtype="i4", fill_value=0)
    crs[...] = 4326
    v = g.create_array("v", shape=(4,), chunks=(2,), dtype="f4", dimension_names=("k",))
    v[:] = 1
    default = H.read_blocks(path)
    assert {r["variable"] for r in default} == {"v"}  # a scalar is not gridded data
    (row,) = H.read_blocks(path, variables="crs")
    assert row["dims"] == [] and row["start"] == [] and row["shape"] == []
    assert int(_read.block_to_numpy(row)) == 4326


def test_the_dependent_selection_is_per_variable(tmp_path):
    """sel on a dimension only some variables have reaches those variables and no error."""
    path = tmp_path / "two.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    a = g.create_array(
        "a", shape=(4, 6), chunks=(2, 3), dtype="f4", dimension_names=("y", "x")
    )
    a[:] = 1
    b = g.create_array("b", shape=(6,), chunks=(3,), dtype="f4", dimension_names=("x",))
    b[:] = 2
    rows = H.read_blocks(path, isel={"y": slice(0, 2)})
    _, seen_a = H.assemble(rows, "a", (4, 6))
    _, seen_b = H.assemble(rows, "b", (6,))
    assert seen_a[:2].all() and not seen_a[2:].any() and seen_b.all()


# --- decoding --------------------------------------------------------------------------------


def test_decode_false_returns_stored_values_and_attributes(tmp_path):
    path = H.packed_store(tmp_path / "p.zarr")
    decoded = H.read_blocks(path, variables="p")
    raw = H.read_blocks(path, variables="p", decode=False)
    got, _ = H.assemble(decoded, "p", (6,), dtype="float64")
    np.testing.assert_array_equal(
        got, [10.0, 11.0, np.nan, 12.0, 13.0, 10.0]
    )  # (v * 0.5) + 10, -1 masked
    stored, _ = H.assemble(raw, "p", (6,), dtype="int16")
    np.testing.assert_array_equal(stored, [0, 2, -1, 4, 6, 0])
    assert {r["dtype"] for r in raw} == {"<i2"} and decoded[0]["dtype"].startswith("<f")
    # decoding consumed the packing attributes; the raw block still carries them
    assert "scale_factor" not in H.json_attrs(decoded[0])
    assert H.json_attrs(raw[0])["scale_factor"] == 0.5
    # a never-written cell holds, in the decoded block's units, the decoded fill value
    assert raw[0]["fill_value"] == "0" and decoded[0]["fill_value"] == "10.0"


def test_use_zarr_fill_value_as_mask_keeps_the_zeros_of_a_v2_integer_array_in_blocks(
    tmp_path,
):
    g = zarr.open_group(str(tmp_path / "z.zarr"), mode="w", zarr_format=2)
    a = g.create_array(
        "a",
        shape=(4,),
        chunks=(2,),
        dtype="i2",
        fill_value=0,
        attributes={"_ARRAY_DIMENSIONS": ["x"]},
    )
    a[:] = np.array([0, 3, 0, 5], dtype="i2")
    path = tmp_path / "z.zarr"
    masked, _ = H.assemble(H.read_blocks(path), "a", (4,), dtype="float64")
    np.testing.assert_array_equal(masked, [np.nan, 3, np.nan, 5])  # xarray's default
    kept = H.read_blocks(path, flag=False)
    got, _ = H.assemble(kept, "a", (4,), dtype="int16")
    np.testing.assert_array_equal(got, [0, 3, 0, 5])
    assert kept[0]["dtype"] == "<i2"


# --- tables ------------------------------------------------------------------------------------


def test_table_by_label_selection_and_a_chosen_variable(era5_v3_sharded):
    cfg, table = H.read_table(
        era5_v3_sharded,
        variables=["u10"],
        sel={"time": slice("2023-01-01T05", "2023-01-01T06"), "latitude": 0.0},
        isel={"longitude": [0, 1]},
    )
    assert [n for n, _ in cfg.columns] == ["time", "latitude", "longitude", "u10"]
    rows = sorted(table.to_pylist(), key=lambda r: (r["time"], r["longitude"]))
    assert [(r["time"].hour, r["latitude"], r["longitude"]) for r in rows] == [
        (5, 0.0, 0.0),
        (5, 0.0, 2.5),
        (6, 0.0, 0.0),
        (6, 0.0, 2.5),
    ]


def test_table_rejects_mixed_dims(era5_v3_sharded):
    with pytest.raises(ZarrGridMismatch) as e:
        H.plan(era5_v3_sharded, output="table", variables=["t2m", "latitude"])
    assert "dimensions" in str(e.value) and "Fix:" in str(e.value)
    assert "same dimensions, or read them separately" in e.value.fix


def test_table_of_variables_with_different_chunking(tmp_path):
    path = tmp_path / "mix.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    for name, chunks in (("a", (2, 3)), ("b", (4, 2))):
        arr = g.create_array(
            name, shape=(4, 6), chunks=chunks, dtype="f8", dimension_names=("y", "x")
        )
        arr[:] = np.arange(24, dtype="f8").reshape(4, 6) * (1 if name == "a" else -1)
    cfg, table = H.read_table(path)
    rows = sorted(table.to_pylist(), key=lambda r: (r["y"], r["x"]))
    assert len(rows) == 24 and [n for n, _ in cfg.columns] == ["y", "x", "a", "b"]
    assert table.schema.field("y").type == pa.int64()  # no coordinates: the index
    np.testing.assert_array_equal([r["a"] for r in rows], np.arange(24.0))
    np.testing.assert_array_equal([r["b"] for r in rows], -np.arange(24.0))


def test_an_empty_selection_plans_no_unit(era5_v3_sharded):
    _, _, units, cfg = H.plan(era5_v3_sharded, sel={"latitude": slice(95, 91)})
    assert units == []
    _, _, units, cfg = H.plan(
        era5_v3_sharded, output="table", sel={"latitude": slice(95, 91)}
    )
    assert units == [] and [n for n, _ in cfg.columns] == [
        "time",
        "latitude",
        "longitude",
        "t2m",
        "u10",
    ]


# --- units and tasks ----------------------------------------------------------------------------


@pytest.mark.parametrize("layout", ["era5_v3_sharded", "era5_v3_unsharded", "era5_v2"])
def test_small_budgets_split_the_read_into_many_tasks_without_losing_a_cell(
    request, monkeypatch, tmp_path, layout
):
    store = request.getfixturevalue(layout)
    src = H.registered_source(monkeypatch, store, overlay=tmp_path)
    monkeypatch.setattr(_read, "_budget_bytes", lambda: 1)  # one chunk per unit
    rows = H.read_blocks(src, variables=["t2m"])
    assert len(rows) == 16  # one block per (variable, chunk)
    got, seen = H.assemble(rows, "t2m", SHAPE)
    assert seen.all()
    np.testing.assert_array_equal(got, H.direct(store, "t2m"))


def test_each_unit_of_a_registered_table_carries_the_rows_of_the_other_variables_chunks(
    tmp_path, monkeypatch
):
    """The first variable's chunks cut the grid; for each unit, the rows of the other
    variable are those of the chunks (here, of the shards) its cells lie in."""
    path = H.make_mixed(tmp_path / "mixed.zarr")
    src = H.registered_source(monkeypatch, path, overlay=tmp_path / "ov")
    monkeypatch.setattr(_read, "_budget_bytes", lambda: 1)  # one chunk of ``a`` a unit
    _, _, units, _ = H.plan(src, variables=["a", "b"], output="table")
    assert len(units) == 16  # a's grid is 4 x 4 chunks
    for unit in units:
        row, col = unit.chunks[0][0], unit.chunks[0][1]
        want_shards = {  # a's columns [3*col, 3*col+3) lie in b's shards of width 4
            f"c/0/{j}" for j in range(3 * col // 4, (3 * col + 2) // 4 + 1)
        }
        a_rows = [r for r in unit.rows if r["array"] == "a"]
        b_rows = [r for r in unit.rows if r["array"] == "b"]
        assert [r["key"] for r in a_rows] == [f"c/{row}/{col}"], (row, col)
        assert {r["key"] for r in b_rows} == want_shards, (row, col)
        assert {r["kind"] for r in b_rows} == {"shard"}


def test_a_table_of_variables_chunked_differently_equals_xarray_without_a_table(
    tmp_path, monkeypatch
):
    """The Delta end-to-end twin is in test_read.py; this is the same read, against xarray."""
    import xarray as xr
    from xarray.backends.zarr import ZarrStore

    path = H.make_mixed(tmp_path / "mixed.zarr")
    src = H.registered_source(monkeypatch, path, overlay=tmp_path / "ov")
    monkeypatch.setattr(_read, "_budget_bytes", lambda: 1)  # many units, merged rows
    cfg, table = H.read_table(src)
    rows = sorted(table.to_pylist(), key=lambda r: (r["y"], r["x"]))
    theirs = xr.open_dataset(
        ZarrStore.open_group(str(path), mode="r", consolidated=False)
    )
    assert [n for n, _ in cfg.columns] == ["y", "x", "a", "b"] and len(rows) == 96
    assert [(r["y"], r["x"]) for r in rows] == [
        (y, x) for y in range(8) for x in range(12)
    ]
    np.testing.assert_array_equal([r["a"] for r in rows], theirs.a.values.reshape(-1))
    np.testing.assert_array_equal([r["b"] for r in rows], theirs.b.values.reshape(-1))
    assert min(abs(r["b"]) for r in rows) >= 1  # no cell read as the fill value


# --- documents the registered store holds ---------------------------------------------------------


@pytest.mark.filterwarnings("ignore:Consolidated metadata")
def test_stale_consolidated_metadata_is_never_served(tmp_path, monkeypatch):
    base = tmp_path / "base.zarr"
    g = zarr.open_group(str(base), mode="w", zarr_format=3)
    z = g.create_array("z", shape=(4,), chunks=(2,), dtype="f4", dimension_names=("k",))
    z[:] = np.arange(4, dtype="f4")
    zarr.consolidate_metadata(str(base))
    z.resize((6,))  # the array's own document moves on; the consolidated one does not
    z[4:] = [10, 11]
    root = json.loads((base / "zarr.json").read_text())
    assert root["consolidated_metadata"]["metadata"]["z"]["shape"] == [4]  # stale

    plain = H.read_blocks(base)
    registered = H.read_blocks(H.registered_source(monkeypatch, base, overlay=tmp_path))
    for rows in (plain, registered):
        got, seen = H.assemble(rows, "z", (6,))
        assert seen.all() and list(got) == [0, 1, 2, 3, 10, 11]  # the per-array shape

    docs = _read.store_documents(
        {
            "zarr.json": json.dumps(root).encode(),
            "z/zarr.json": b"{}",
            ".zmetadata": b"{}",
        }
    )
    assert "consolidated_metadata" not in json.loads(docs["zarr.json"])
    assert ".zmetadata" not in docs and docs["z/zarr.json"] == b"{}"


@pytest.mark.parametrize("source", ["path", "registered"])
def test_an_undecodable_array_raises_only_when_it_is_decoded(
    tmp_path, monkeypatch, caplog, source
):
    path = H.store_with_a_string_coordinate(tmp_path / "st.zarr")
    src = (
        path
        if source == "path"
        else H.registered_source(monkeypatch, path, overlay=tmp_path / "ov")
    )
    with caplog.at_level(logging.WARNING, logger=_read.__name__):
        rows = H.read_blocks(src)  # the rest reads
    got, seen = H.assemble(rows, "v", (3,))
    assert seen.all() and list(got) == [1, 2, 3]
    assert rows[0]["coord_start"] == [None]  # no label for a string coordinate
    assert any("station" in r.getMessage() for r in caplog.records)
    # asking for the array, or for labels from it, names it
    with pytest.raises(ZarrUnsupported) as e:
        H.plan(src, variables=["station"])
    assert "'station'" in str(e.value) and "Fix:" in str(e.value)
    with pytest.raises(ZarrUnsupported) as e:
        H.plan(src, sel={"station": "a"})
    assert "'station'" in str(e.value)
    with pytest.raises(ZarrUnsupported) as e:
        H.plan(src, output="table")
    assert "'station'" in str(e.value)
    by_position = H.read_blocks(src, isel={"station": [0, 2]})
    _, seen = H.assemble(by_position, "v", (3,))
    assert list(seen) == [True, False, True]


@pytest.mark.parametrize("layout", ["era5_v3_unsharded", "era5_v2"])
def test_a_managed_store_reads_from_its_data_root(request, monkeypatch, layout):
    store = request.getfixturevalue(layout)  # chunk rows, v3 and v2
    src = H.registered_source(monkeypatch, store, origin="gbx")
    got, seen = H.assemble(H.read_blocks(src, variables=["t2m"]), "t2m", SHAPE)
    assert seen.all()
    np.testing.assert_array_equal(got, H.direct(store, "t2m"))


# --- arguments are checked before any work ----------------------------------------------------------


def test_arguments_are_checked_before_any_work(era5_v3_sharded, era5_v2):
    """No session is given: every one of these is refused on the driver, before a plan."""
    read, path = _read.gbx_zarr_read, str(era5_v3_sharded)
    with pytest.raises(ValueError, match="output"):
        read(None, path, output="rows")
    with pytest.raises(ZarrAccessUnavailable) as e:
        read(None, path, access="direct")
    assert 'access="auto"' in e.value.fix
    with pytest.raises(ValueError, match="access"):
        read(None, path, access="turbo")
    with pytest.raises(ValueError, match="band_dim"):
        read(None, path, band_dim="time")
    with pytest.raises(ValueError, match="tile_size"):
        read(None, path, tile_size=(256, 256))
    with pytest.raises(ValueError, match="not both"):
        read(None, path, version_as_of=1, timestamp_as_of="2020-01-01")
    with pytest.raises(ZarrVersionUnavailable) as e:  # a plain store has no versions
        read(None, str(era5_v2), version_as_of=1)
    assert "Fix:" in str(e.value) and "gbx_zarr_register" in e.value.fix
    with pytest.raises(ValueError, match="unknown variable"):
        read(None, path, variables=["nope"])
    with pytest.raises(ValueError, match="unknown dimension"):
        read(None, path, sel={"depth": 1})
    with pytest.raises(ValueError, match="both sel and isel"):
        read(None, path, sel={"time": "2023-01-01"}, isel={"time": 0})
    with pytest.raises(ValueError, match="use_zarr_fill_value_as_mask"):
        read(None, path, use_zarr_fill_value_as_mask="no")
