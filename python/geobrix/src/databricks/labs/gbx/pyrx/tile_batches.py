"""Decode virtual tiles in bounded Arrow batches: the batched decode of the light tier.

A bytes-free (virtual) tile carries a path and a window, not bytes. ``materialize_batches`` turns the virtual tiles of an
Arrow batch into tiles with bytes in ONE Python call (no per-row UDF hop), and hands them on in slices that hold at most
``out_chunk_bytes()`` of decoded bytes each. The ``mapInArrow`` body of a bench form and the reader's decode at read
(``raster_gbx`` with ``virtualTiles=false``) both run it.

Every step that can fail on a tile's own account (a bad file, a refused location, a tile over the read cap) gives an
error tile, the reason under ``metadata["last_error"]`` (the convention of the light ``rst_*`` functions); resource
exhaustion (``MemoryError``, ``RecursionError``) still fails the task.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

import os
from types import SimpleNamespace

import numpy as np

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import budget
from databricks.labs.gbx.pyrx.core import open_tile as ot

# One file header read per file version per worker: (path, size, mtime) -> (width, height, count, dtype, crs).
_HEADERS: dict = {}
_HEADERS_MAX = 4096


def _stamp(path: str):
    """The key of a file's version: a rewritten file is another key, so it is never served another file's header."""
    from databricks.labs.gbx.ds._listing import to_local_path

    try:
        st = os.stat(to_local_path(path))
    except OSError:  # not a local file (the gate judges it when the header is opened)
        return (path, None, None)
    return (path, st.st_size, st.st_mtime_ns)


def source_header(path: str):
    """(width, height, count, dtype, crs) of the file at `path`, opened as every light read is (the driver policy and
    the remote-path gate). Cached per worker: the many windows of one file read its header once.
    """
    key = _stamp(path)
    hit = _HEADERS.get(key)
    if hit is None:
        from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

        with ot.open_header(VirtualTile(cellid=0, path=path)) as h:
            hit = (h.width, h.height, h.count, str(h.dtypes[0]), h.crs)
        if len(_HEADERS) >= _HEADERS_MAX:
            _HEADERS.clear()
        _HEADERS[key] = hit
    return hit


def decoded_bytes(tile) -> int:
    """What decoding `tile` holds in memory: the pixels of its window, of the bands it keeps (a pending band select),
    doubled when a pending reprojection holds the window it reads and the warped copy of it. A Zarr-backed tile has no
    file header: its own header view (window and bands applied) is what `open_header` gives.
    """
    vt = ot._to_virtual_tile(tile)
    if ot._zarr_tiles.is_zarr_tile(vt):
        with ot.open_header(vt) as h:
            return h.width * h.height * h.count * int(np.dtype(h.dtypes[0]).itemsize)
    width, height, count, dtype, crs = source_header(vt.path)
    if vt.window is not None:
        _, _, width, height = vt.window
    bands = ot._parse_pending(vt.metadata)[0]
    size = width * height * (len(bands) if bands else count)
    size *= int(np.dtype(dtype).itemsize)
    return (
        size * 2 if vt.crs and ot._warp_pending(vt, SimpleNamespace(crs=crs)) else size
    )


def refuse_over_cap(size: int, cap_bytes, what: str) -> None:
    """The materialize gate of a worker (`materialize_decision`, kind `ingest`): `size` bytes held over this compute's cap
    fail fast with their fix, never an OOM. `cap_bytes` is the driver's connect-aware cap (a worker cannot measure it).
    """
    from databricks.labs.gbx.ds.file_gbx import materialize_decision

    if materialize_decision(size, "ingest", cap_bytes=cap_bytes) == "error":
        raise ValueError(
            f"{what} is {size // 2**20} MiB, over this compute's read cap. "
            "Fix: retile it first (gbx_rst_retile) so each tile is smaller"
        )


def gate(tile, cap_bytes) -> None:
    """`refuse_over_cap` for the decoded size of a virtual tile."""
    refuse_over_cap(decoded_bytes(tile), cap_bytes, "the virtual tile, decoded,")


def decode_py(tile, cap_bytes) -> dict:
    """The materialized row of a virtual tile through pyrx (`open_tile`: the driver policy, the remote-path gate, FUSE
    staging): the bytes, the pending instructions consumed, and `path` cleared, as the JVM's decode leaves the tile.
    """
    gate(tile, cap_bytes)
    row = ot.materialize_to_bytes(ot._to_virtual_tile(tile)).to_row()
    row["path"] = None
    return row


def cellid_of(tile) -> int:
    """The cell id of a tile row (0 when it has none)."""
    return int((tile.get("cellid") if hasattr(tile, "get") else None) or 0)


def materialized(tile, cap_bytes, who: str = "materialize", decode=None) -> dict:
    """The row for a virtual tile: decoded (`decode`, default `decode_py`), or an error tile when it cannot be (a bad
    file, a refused location, a tile over the cap), the reason under `metadata["last_error"]` as the JVM's `safeEval`
    gives it. Resource exhaustion fails the task.
    """
    try:
        return (decode or decode_py)(tile, cap_bytes)
    except (MemoryError, RecursionError):
        raise
    except Exception as e:  # noqa: BLE001
        return _serde.build_error_tile(f"{who}: {e}", cellid_of(tile))


# The decoded bytes one output batch may hold. A batch of every tile of an Arrow batch (1000 tiles of 4 MiB) is a
# 4 GiB column: Arrow cannot build a binary column past 2 GiB ("Cannot convert ChunkedArray to Array"), and a
# Serverless UDF is limited to 1 GiB.
_OUT_CHUNK_BYTES = 64 << 20


def out_chunk_bytes() -> int:
    """The most decoded bytes one output batch holds: 64 MiB, and never more than this runtime's decoded budget."""
    return min(_OUT_CHUNK_BYTES, budget.decoded_budget_bytes("auto"))


def materialize_batches(batches, cap_bytes=None, decode=None):
    """Decode every virtual tile of an Arrow batch in one Python call (no per-row UDF hop): the `mapInArrow` body over
    a one-column `tile` DataFrame. `cap_bytes` is the driver's read cap (`report_detected_cap(spark)`).
    `decode(tile, cap_bytes)` is the per-tile step (default `materialized`); a bench floor form passes `keep_virtual`
    to time the shell alone.

    A map column is a list of (key, value) pairs in `to_pylist()`: each tile's metadata is made a dict once for the
    decode, and written back in the schema's form. Rows leave in order, in slices of at most `out_chunk_bytes()` of
    decoded bytes (a tile alone over it is a slice of its own).
    """
    import pyarrow as pa

    bound = out_chunk_bytes()
    for b in batches:
        rows = b.to_pylist()
        start, held = 0, 0
        for i, r in enumerate(rows):
            t = r["tile"]
            if t is None or t["raster"] is not None or t["path"] is None:
                continue
            t["metadata"] = dict(t["metadata"] or ())
            out = (decode or materialized)(t, cap_bytes)
            out["metadata"] = list((out["metadata"] or {}).items())
            r["tile"] = out
            size = len(out["raster"] or b"")
            if held and held + size > bound:
                # this tile would take the slice past the bound: send what is held first
                yield pa.RecordBatch.from_pylist(rows[start:i], schema=b.schema)
                for done in rows[start:i]:
                    done["tile"] = None  # let the decoded bytes go
                start, held = i, 0
            held += size
        if start < len(rows) or not rows:
            yield pa.RecordBatch.from_pylist(rows[start:], schema=b.schema)
