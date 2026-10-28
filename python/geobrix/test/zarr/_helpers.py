"""Helpers for the Zarr tests that run a read without Spark.

A ``gbx_zarr_read`` is two halves. The driver plans (``_read._plan``: which chunks, which
rows) and each Spark task runs one unit of that plan (``_read._task_function``). ``run``
feeds a plan's units to the task body one at a time, in this process, as Spark would, so a
correctness matrix (selections, dtypes, CF decoding, layouts) takes milliseconds a case
instead of a Spark job. What it leaves out is the work Spark itself does: distributing the
units, and fetching manifest rows from a Delta table. ``registered_source`` stands in for
that second part with a plain store's own files as manifest rows.

The Delta tests keep one end-to-end test per behaviour (``test_read.py``); these helpers
carry the matrix around it.
"""

import dataclasses
import json

import numpy as np
import pyarrow as pa


def plan(
    source,
    variables=None,
    sel=None,
    isel=None,
    output="blocks",
    decode=True,
    flag=None,
    access=None,
):
    """``(src, names, units, cfg)`` of a read of ``source``: a path (a plain store) or a
    ``registered_source``. ``access`` is the ``AccessConfig`` its tasks read with."""
    from databricks.labs.gbx.zarr import _read

    src = (
        _read._open_source(None, str(source), "path", None, None)
        if isinstance(source, (str, bytes)) or hasattr(source, "__fspath__")
        else source
    )
    names = _read._choose_variables(variables, src)
    units, cfg = _read._plan(None, src, names, sel, isel, output, decode, flag)
    return src, names, units, dataclasses.replace(cfg, access=access)


def run(units, cfg):
    """Every unit through the task body, as one Spark task each; the batches in order.

    The task function is pickled and unpickled first, as Spark ships it, so a plan that
    only works because it shares memory with the driver fails here. (The pickle holds this
    process's own closure and is read back at once; nothing comes from outside.)
    """
    from pyspark import cloudpickle

    from databricks.labs.gbx.zarr import _read

    task = cloudpickle.loads(cloudpickle.dumps(_read._task_function(cfg)))
    out = []
    for i, unit in enumerate(units):
        batch = pa.record_batch(
            {"id": pa.array([i], pa.int64()), "unit": pa.array([unit.to_json()])}
        )
        out += list(task([batch]))
    return out


def read_blocks(source, **kw):
    """The block rows (dicts) of a read of ``source``, as ``gbx_zarr_read(...).collect()``."""
    _, _, units, cfg = plan(source, output="blocks", **kw)
    return [r for b in run(units, cfg) for r in b.to_pylist()]


def read_table(source, **kw):
    """``(cfg, pa.Table)`` of a table-output read of ``source``."""
    from databricks.labs.gbx.zarr import _read

    _, _, units, cfg = plan(source, output="table", **kw)
    schema = pa.schema([(n, _read._TABLE_ARROW[k]) for n, k in cfg.columns])
    batches = run(units, cfg)
    table = (
        pa.Table.from_batches(batches, schema=schema)
        if batches
        else schema.empty_table()
    )
    return cfg, table


def read_variable(spark, table, name, version_as_of=None):
    """Variable ``name`` of the store of manifest ``table``, assembled from its blocks (NaN,
    or NaT, where no block).

    The blocks come from the read's own planner and task body, run in this process
    (``run``): the manifest is queried for real, but no Spark job decodes the chunks, which
    is most of a read's cost on a local session. The Spark-distributed read has its
    end-to-end tests in test_read.py.
    """
    from databricks.labs.gbx.zarr import _read
    from databricks.labs.gbx.zarr._read import block_to_numpy

    src = _read._open_source(spark, table, "table", version_as_of, None)
    names = _read._choose_variables([name], src)
    units, cfg = _read._plan(spark, src, names, None, None, "blocks", True, None)
    rows = [r for b in run(units, cfg) for r in b.to_pylist()]
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


DIMS = ("time", "latitude", "longitude")


def blocks_frame(spark, *blocks):
    """A blocks DataFrame of ``(variable, start, values)``; values are decoded values."""
    from databricks.labs.gbx.zarr._read import BLOCKS_SCHEMA

    rows = []
    for name, start, values in blocks:
        values = np.ascontiguousarray(values)
        rows.append(
            (
                name,
                [name] if name in DIMS else list(DIMS[-values.ndim :]),
                list(start),
                [a + n for a, n in zip(start, values.shape)],
                None,
                None,
                values.dtype.str,
                list(values.shape),
                values.tobytes(),
                None,
                None,
            )
        )
    return spark.createDataFrame(rows, BLOCKS_SCHEMA)


def assemble(rows, variable, shape, dtype="float32"):
    """The blocks of ``variable`` laid back into an array of ``shape``, and a mask of the
    cells they cover (NaN, or 0, where no block)."""
    from databricks.labs.gbx.zarr._read import block_to_numpy

    out = np.zeros(shape, dtype=dtype)
    if out.dtype.kind == "f":
        out[...] = np.nan
    seen = np.zeros(shape, dtype=bool)
    for r in rows:
        if r["variable"] != variable:
            continue
        box = tuple(slice(a, b) for a, b in zip(r["start"], r["stop"]))
        block = block_to_numpy(r)
        assert (
            block.shape
            == tuple(r["shape"])
            == tuple(b - a for a, b in zip(r["start"], r["stop"]))
        )
        assert not seen[box].any(), "blocks overlap"
        out[box] = block
        seen[box] = True
    return out, seen


def mask(shape, *index_lists):
    """A boolean array of ``shape``, true on the outer product of ``index_lists``."""
    out = np.zeros(shape, dtype=bool)
    out[np.ix_(*index_lists)] = True
    return out


def direct(store, name):
    """Array ``name`` of the store at ``store``, read by zarr itself."""
    import zarr

    return zarr.open_array(str(store / name), mode="r")[...]


# --- a registered or managed store, without a Delta table ------------------------------

_ROW_COLUMNS = (
    "kind array key shard_key file offset length checksum origin identity write_id tag meta"
).split()


def _fake_read_rows(all_rows):
    """A ``_manifest.read_rows`` over rows held in memory: the same filters, no Delta."""

    def read_rows(spark, snap, *, kinds=None, arrays=None, shard_keys=None, keys=None):
        def as_set(value):
            return (
                None
                if value is None
                else {value} if isinstance(value, str) else set(value)
            )

        kinds, arrays = as_set(kinds), as_set(arrays)
        shard_keys, keys = as_set(shard_keys), as_set(keys)
        out = []
        for r in all_rows:
            if kinds is not None and r["kind"] not in kinds:
                continue
            if arrays is not None and r["array"] not in arrays:
                continue
            if shard_keys is not None and r.get("shard_key") not in shard_keys:
                continue
            if keys is not None and r["key"] not in keys:
                continue
            out.append(r)
        return pa.Table.from_pylist([{c: r.get(c) for c in _ROW_COLUMNS} for r in out])

    return read_rows


def _plain_rows(store, origin):
    from databricks.labs.gbx.zarr._testing import plain_store_rows

    return plain_store_rows(store, origin=origin)


def _register_rows(store, origin):
    """``(metas, rows, array names)`` as ``gbx_zarr_register`` builds them, tolerating
    arrays GeoBrix cannot decode (their documents are kept and their chunks get rows).
    """
    from pathlib import Path

    from databricks.labs.gbx.zarr import _register

    metas, arrays, unsupported = _register.read_base_metadata(str(store), tolerant=True)
    layouts = {a: _register._layout_of(m) for a, m in arrays.items()}
    layouts.update({a: _register._layout_of_unsupported(a, metas) for a in unsupported})
    files = sorted(f for f in Path(store).rglob("*") if f.is_file())
    objects = pa.table(
        {
            "relpath": [f.relative_to(store).as_posix() for f in files],
            "length": [f.stat().st_size for f in files],
            "mtime_ms": [f.stat().st_mtime_ns // 1_000_000 for f in files],
        }
    )
    rows, _ = _register._chunk_rows(objects, layouts, "a" * 32)
    rows = [{**r, "origin": origin} for r in rows.to_pylist()]
    return metas, rows, set(arrays) | set(unsupported)


def registered_source(
    monkeypatch, store, *, overlay=None, rows=None, origin="external"
):
    """A table-kind ``_Source`` over the local store ``store``, as a read would open it.

    The manifest rows are those of the store's own files (``plain_store_rows``), or
    ``rows`` when given (for example with one inner chunk rewritten into ``overlay``).
    ``origin="external"`` is a registered store (its files are the base, ``overlay`` is
    where GeoBrix wrote); ``origin="gbx"`` is a managed store, whose data root is ``store``.
    ``_manifest.read_rows`` is replaced, through ``monkeypatch``, by a filter over those
    rows, so ``_read`` plans exactly as it does against a table.
    """
    from databricks.labs.gbx.zarr import _manifest, _read
    from databricks.labs.gbx.zarr.errors import ZarrUnsupported

    try:
        metas, base_rows, arrays = _plain_rows(store, origin)
        arrays = set(arrays)
    except (
        ZarrUnsupported
    ):  # an array GeoBrix cannot decode: register's own row builder
        metas, base_rows, arrays = _register_rows(store, origin)
    rows = list(base_rows if rows is None else rows)
    docs = []
    for key, raw in sorted(metas.items()):
        prefix, _, name = key.rpartition("/")
        docs.append(
            dict(
                kind="array" if prefix in arrays else "group",
                array=prefix or None,
                key=name,
                meta=raw.decode(),
            )
        )
    monkeypatch.setattr(_manifest, "read_rows", _fake_read_rows(rows + docs))
    held = {
        "/".join(p for p in (d["array"], d["key"]) if p): d["meta"].encode()
        for d in docs
    }
    parsed, unsupported = _read.parse_documents(held)
    managed = origin == "gbx"
    return _read._Source(
        "table",
        str(store) if managed else str(overlay),
        None if managed else str(store),
        _read.store_documents(held),
        parsed,
        unsupported,
        spark=None,
        table="t",
        snap=None,
    )


def sample_table(
    source,
    points,
    *,
    crs=None,
    engine="auto",
    variables=None,
    sel=None,
    flag=None,
    access=None,
):
    """``(cfg, pa.Table)`` of ``gbx_zarr_sample`` at the geometries ``points``, without Spark.

    ``source`` is a path or a ``registered_source``. The driver's planning and mapping and a
    task's reading run in this process, as the sampler runs them; the table has a ``point``
    column (the position in ``points``) before the sample's columns, in (point, step) order.
    """
    from databricks.labs.gbx.zarr import _read, _sample

    src = (
        _read._open_source(None, str(source), "path", None, None)
        if isinstance(source, (str, bytes)) or hasattr(source, "__fspath__")
        else source
    )
    names = _read._choose_variables(variables, src)
    cfg = _sample._plan_sample(src, names, sel, crs, flag)
    cfg.read = dataclasses.replace(cfg.read, access=access)
    uid, row, col = cfg.grid.map(points, crs)
    units = _sample._units(src, cfg, np.unique(uid[uid >= 0]))
    parts = []
    for point, extra in _sample._sample_points(cfg, units, uid, row, col, engine):
        parts.append(
            pa.Table.from_batches([extra]).add_column(0, "point", pa.array(point))
        )
    table = (
        pa.concat_tables(parts)
        if parts
        else pa.schema([("point", pa.int64()), *cfg.extra_schema]).empty_table()
    )
    order = np.argsort(table.column("point").to_numpy(), kind="stable")
    return cfg, table.take(pa.array(order))


def json_attrs(row):
    """The attributes of a block row as a dict."""
    return json.loads(row["attrs"] or "{}")


# --- small stores several tests read -----------------------------------------------------


def make_mixed(path):
    """A v3 store whose two variables have different chunks, and ``b`` shards.

    ``a`` has chunks (2, 3) and no shards; ``b`` has chunks (4, 2) inside shards (8, 4), so
    one chunk of ``a`` lies in two chunks, often of two shards, of ``b``. No value is 0 (the
    fill), so a chunk read from the wrong place, or not at all, is seen. ``a`` is
    ``1..96`` and ``b`` its negative, both ``(8, 12)`` over dimensions ``y`` and ``x``.
    """
    import zarr

    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    data = np.arange(1, 97, dtype="f8").reshape(8, 12)
    a = g.create_array(
        "a", shape=(8, 12), chunks=(2, 3), dtype="f8", dimension_names=("y", "x")
    )
    b = g.create_array(
        "b",
        shape=(8, 12),
        chunks=(4, 2),
        shards=(8, 4),
        dtype="f8",
        dimension_names=("y", "x"),
    )
    a[:] = data
    b[:] = -data
    return path


def packed_store(path, zarr_format=3):
    """int16 with scale_factor/add_offset and a _FillValue, and a v2-style zero fill_value."""
    import zarr

    g = zarr.open_group(str(path), mode="w", zarr_format=zarr_format)
    attrs = {"scale_factor": 0.5, "add_offset": 10.0, "_FillValue": -1}
    if zarr_format == 2:
        attrs["_ARRAY_DIMENSIONS"] = ["x"]
    a = g.create_array(
        "p",
        shape=(6,),
        chunks=(3,),
        dtype="i2",
        fill_value=0,
        attributes=attrs,
        **({"dimension_names": ("x",)} if zarr_format == 3 else {}),
    )
    a[:] = np.array([0, 2, -1, 4, 6, 0], dtype="i2")
    return path


def store_with_a_string_coordinate(path):
    """A float variable ``v`` over ``station``, whose coordinate is strings (undecodable)."""
    import zarr

    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    v = g.create_array(
        "v", shape=(3,), chunks=(2,), dtype="f4", dimension_names=("station",)
    )
    v[:] = np.array([1, 2, 3], dtype="f4")
    s = g.create_array(
        "station", shape=(3,), chunks=(2,), dtype=str, dimension_names=("station",)
    )
    s[:] = np.array(["a", "bb", "ccc"], dtype=object)
    return path
