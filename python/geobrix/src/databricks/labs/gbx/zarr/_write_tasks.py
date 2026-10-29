"""The two Spark stages of ``gbx_zarr_write``: what runs in the tasks.

**Explode** (``explode_function``) cuts each input block into the inner chunks it touches.
A block that covers a whole chunk is encoded right there, so only encoded bytes cross the
shuffle; any other piece travels raw (stored values). A coordinate block (one dimension,
named like it) becomes a side row the driver collects: coordinates are written on the
driver at commit. Each task also emits one digest row: how many blocks it saw, a hash of
their positions, the region they cover and the variables they hold.

**Assemble** (``assemble_function``) receives, for each group ``(variable, part)`` (a
shard, or a chunk of an unsharded array), the manifest rows of that group at the base
version (joined in Spark, ``ord = 0``) and then the pieces sorted by chunk key
(``ord = 1``). It assembles each chunk. A chunk covered only in part is read from the
group's base rows through a ``ManifestStore`` (read-modify-write), and the shard row it
was read from is reported as a guard: the commit checks it is unchanged. Every chunk is
appended to the task's pack file and reported with the base ``write_id`` of its key (the
compare-and-swap expectation) and whether a live shard row holds it. In an overwrite the
group's base rows that no piece rewrote are reported as deletes. Side rows pass through.

**Pack** (``pack_function``) is the one stage of a create whose blocks each cover whole
units (shards of a sharded array, chunks otherwise) and no cell twice: every piece is a
whole chunk, so the task that makes a block encodes it and appends it to its own pack file,
as the assembler would, and nothing is shuffled. Its rows are the assembler's: the same
chunk rows (only the pack a chunk lands in differs), coordinate side rows and one digest row.

Nothing here needs a session: the tasks see only their rows and the closure.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import os
from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from databricks.labs.gbx.zarr._keys import shard_of
from databricks.labs.gbx.zarr._meta import ArrayMeta, cf_encode, is_time_coord
from databricks.labs.gbx.zarr._pack import (
    PackWriter,
    crc32c_hex,
    encode_inner_chunk,
    is_fill,
    pad_to_chunk,
)
from databricks.labs.gbx.zarr.errors import ZarrError, ZarrGridMismatch, ZarrUnsupported

__all__ = [
    "PIECES_DDL",
    "ROWS_DDL",
    "ROWS_ARROW",
    "TaskConfig",
    "assemble_function",
    "block_hash",
    "block_is_stored",
    "chunk_grid",
    "explode_function",
    "extent",
    "pack_function",
    "part_of",
]

_CF_ATTRS = ("scale_factor", "add_offset", "_FillValue", "missing_value")
_TIME_ATTRS = ("units", "calendar")
_HASH_MASK = (1 << 64) - 1
_SIDE_PART = "side"

PIECES_DDL = (
    "variable STRING, key STRING, part STRING, shard_key STRING, ord INT, "
    "chunk ARRAY<BIGINT>, kind STRING, lo ARRAY<BIGINT>, shape ARRAY<BIGINT>, "
    "data BINARY, empty BOOLEAN, row STRING"
)
_PIECES_ARROW = pa.schema(
    [
        ("variable", pa.string()),
        ("key", pa.string()),
        ("part", pa.string()),
        ("shard_key", pa.string()),
        ("ord", pa.int32()),
        ("chunk", pa.list_(pa.int64())),
        ("kind", pa.string()),
        ("lo", pa.list_(pa.int64())),
        ("shape", pa.list_(pa.int64())),
        ("data", pa.binary()),
        ("empty", pa.bool_()),
        ("row", pa.string()),
    ]
)
ROWS_DDL = (
    "rtype STRING, kind STRING, array STRING, key STRING, shard_key STRING, "
    "file STRING, offset BIGINT, length BIGINT, checksum STRING, empty BOOLEAN, "
    "rmw BOOLEAN, expected_write_id STRING, identity STRING, shard_live BOOLEAN, "
    "info STRING, "
    "data BINARY"
)
ROWS_ARROW = pa.schema(
    [
        ("rtype", pa.string()),  # chunk | guard | delete | coord | digest
        ("kind", pa.string()),
        ("array", pa.string()),
        ("key", pa.string()),
        ("shard_key", pa.string()),
        ("file", pa.string()),
        ("offset", pa.int64()),
        ("length", pa.int64()),
        ("checksum", pa.string()),
        ("empty", pa.bool_()),
        ("rmw", pa.bool_()),
        ("expected_write_id", pa.string()),
        ("identity", pa.string()),  # of the base object a chunk row shadows
        ("shard_live", pa.bool_()),
        ("info", pa.string()),
        ("data", pa.binary()),
    ]
)


@dataclass
class TaskConfig:
    """What the tasks need; travels in the ``mapInArrow`` closures (no manifest rows)."""

    write_id: str
    mode: str
    root: str  # where this write's files go, and gbx-origin files resolve
    base_root: str | None  # a registered store's own files
    arrays: dict  # name -> ArrayMeta after the write: the data arrays a block may name
    offsets: dict  # dim -> added to an input position
    base_docs: dict  # documents at the base of the arrays a chunk may be read from
    base_arrays: dict  # name -> ArrayMeta at the base
    unknown_fix: str  # the Fix: line for a block of an array the write did not plan
    batch_rows: int
    batch_bytes: int
    roll_bytes: int
    fault: tuple | None = None
    trace: bool = False  # report per-task placement and per-pack timings (_Trace)
    unsupported: dict | None = None  # name -> why the store cannot decode that array
    access: object = None  # AccessConfig: how the files are reached (holds credentials)


# --- shared helpers ----------------------------------------------------------------------


def block_hash(variable, start, stop) -> int:
    """A 64-bit hash of a block's position. The digest of a block set is the count and
    the sum of these (mod 2**64), so it does not depend on the order blocks arrive in.
    """
    text = f"{variable}\x1f{','.join(map(str, start))}\x1f{','.join(map(str, stop))}"
    return int.from_bytes(
        hashlib.blake2b(text.encode(), digest_size=8).digest(), "little"
    )


def block_is_stored(attrs_text, meta: ArrayMeta) -> bool:
    """Whether a block holds stored values: ``gbx_zarr_read(decode=True)`` drops the CF
    attributes its decoding consumed, ``decode=False`` keeps them. A block that still
    carries one of the array's packing or fill attributes (or a time coordinate's units)
    is therefore stored values; anything else is decoded values, to be encoded."""
    if not attrs_text:
        return False
    try:
        attrs = json.loads(attrs_text)
    except ValueError:
        return False
    if not isinstance(attrs, dict):
        return False
    keys = [k for k in _CF_ATTRS if k in meta.attrs]
    if is_time_coord(meta):
        keys += [k for k in _TIME_ATTRS if k in meta.attrs]
    return any(k in attrs for k in keys)


def chunk_grid(meta: ArrayMeta, start, stop):
    """The chunk coordinates a box ``[start, stop)`` touches."""
    ranges = [
        range(a // c, (b - 1) // c + 1) if b > a else range(0)
        for a, b, c in zip(start, stop, meta.chunks)
    ]
    return itertools.product(*ranges)


def extent(meta: ArrayMeta, chunk) -> tuple[int, ...]:
    """Cells of ``chunk`` inside the array (an edge chunk has fewer)."""
    return tuple(
        max(0, min(c, n - k * c)) for k, c, n in zip(chunk, meta.chunks, meta.shape)
    )


def part_of(meta: ArrayMeta, chunk) -> tuple[str, str | None]:
    """``(part, shard_key)``: what a chunk is grouped by in Spark, and its shard."""
    if meta.shard is not None:
        key = meta.key_encoding.chunk_key(shard_of(chunk, meta.shard.cps))
        return key, key
    return meta.key_encoding.chunk_key(chunk), None


def _encode(cfg: TaskConfig, meta: ArrayMeta, key: str, values) -> bytes:
    if cfg.fault is not None and tuple(cfg.fault) == (meta.name, key):
        raise ZarrError(
            f"test fault while encoding {meta.name}/{key}",
            fix="clear databricks.labs.gbx.zarr._write._FAULT",
        )
    return encode_inner_chunk(meta, values)


def _batch(rows: list[dict], schema: pa.Schema) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(rows, schema=schema)


def _native(meta: ArrayMeta) -> np.dtype:
    return np.dtype(meta.dtype).newbyteorder("=")


def _fill_chunk(meta: ArrayMeta) -> np.ndarray:
    fill = 0 if meta.fill_value is None else meta.fill_value
    return np.full(meta.chunks, fill, dtype=_native(meta))


# --- explode -------------------------------------------------------------------------------


def explode_function(cfg: TaskConfig):
    """Blocks to pieces, plus coordinate side rows and one digest row per task."""

    def run(batches):
        out, held = [], 0
        seen = {"count": 0, "hash": 0, "region": {}, "variables": set()}
        for batch in batches:
            table = pa.Table.from_batches([batch])
            cols = ["variable", "dims", "start", "stop", "dtype", "shape"]
            cols += [c for c in ("attrs",) if c in table.schema.names]
            rows = table.select(cols).to_pylist()
            data = batch.column("data")
            for i, row in enumerate(rows):
                seen["count"] += 1
                h = block_hash(row["variable"], row["start"] or (), row["stop"] or ())
                seen["hash"] = (seen["hash"] + h) & _HASH_MASK
                for piece in _block_pieces(cfg, row, data[i].as_buffer(), seen):
                    out.append(piece)
                    held += len(piece["data"] or b"")
                    if len(out) >= cfg.batch_rows or held >= cfg.batch_bytes:
                        yield _batch(out, _PIECES_ARROW)
                        out, held = [], 0
        info = {
            "count": seen["count"],
            "hash": str(seen["hash"]),
            "region": seen["region"],
            "variables": sorted(seen["variables"]),
        }
        out.append(_side("digest", json.dumps(info), None))
        yield _batch(out, _PIECES_ARROW)

    return run


def _side(kind: str, info: str, data) -> dict:
    """A row that only passes through the assemble stage to the driver."""
    return {
        "variable": "",
        "key": "",
        # one fixed group: a task Spark recomputes (after a lost shuffle file) routes its
        # side rows exactly as before, so none is lost or seen twice
        "part": _SIDE_PART,
        "shard_key": None,
        "ord": 2,
        "chunk": None,
        "kind": kind,
        "lo": None,
        "shape": None,
        "data": data,
        "empty": None,
        "row": info,
    }


def _grow(region: dict, dim: str, a: int, b: int) -> None:
    lo_hi = region.setdefault(dim, [a, b])
    lo_hi[0], lo_hi[1] = min(lo_hi[0], a), max(lo_hi[1], b)


def _block_pieces(cfg: TaskConfig, row: dict, buf, seen: dict):
    name = row["variable"]
    dims = list(row["dims"] or ())
    dtype = np.dtype(row["dtype"])
    shape = tuple(row["shape"] or ())
    start_in = tuple(row["start"] or ())
    if buf is None or len(buf) != math.prod(shape) * dtype.itemsize:
        raise ZarrError(
            f"a block of {name!r} holds {0 if buf is None else len(buf)} bytes, not the "
            f"{math.prod(shape) * dtype.itemsize} its shape {shape} and dtype {dtype} need",
            fix="give each block data of exactly shape x itemsize bytes",
        )
    if dims == [name]:  # a coordinate: written on the driver at commit
        at = start_in[0] + cfg.offsets.get(name, 0)
        _grow(seen["region"], name, at, at + shape[0])
        info = {
            "variable": name,
            "start": start_in[0],
            "stop": start_in[0] + shape[0],
            "dtype": row["dtype"],
            "attrs": row.get("attrs"),
        }
        yield _side("coord", json.dumps(info), bytes(buf))
        return
    meta = cfg.arrays.get(name)
    if meta is None and name in (cfg.unsupported or {}):
        raise ZarrUnsupported(
            f"array {name!r} of the store cannot be decoded, so it cannot be written: "
            f"{cfg.unsupported[name]}",
            fix=f"leave the blocks of {name!r} out of the input, or rewrite that array "
            "in a dtype GeoBrix decodes",
        )
    if meta is None:
        raise ZarrError(
            f"the blocks hold the variable {name!r}, which this write did not plan",
            fix=cfg.unknown_fix,
        )
    if tuple(dims) != tuple(meta.dims):
        raise ZarrGridMismatch(
            f"a block of {name!r} has dimensions {tuple(dims)}, the array {meta.dims}",
            fix="write the variable on the store's dimensions, in their order",
        )
    start = tuple(a + cfg.offsets.get(d, 0) for d, a in zip(meta.dims, start_in))
    stop = tuple(a + n for a, n in zip(start, shape))
    if any(a < 0 or b > n for a, b, n in zip(start, stop, meta.shape)):
        raise ZarrGridMismatch(
            f"a block of {name!r} spans {start}..{stop}, outside the array's shape "
            f"{meta.shape}",
            fix='write blocks within the store\'s extent, or use mode="append" to extend '
            "it",
        )
    for d, a, b in zip(meta.dims, start, stop):
        _grow(seen["region"], d, a, b)
    seen["variables"].add(name)
    values = np.frombuffer(buf, dtype=dtype).reshape(shape)
    stored = cf_encode(values, meta, stored=block_is_stored(row.get("attrs"), meta))
    for chunk in chunk_grid(meta, start, stop):
        origin = tuple(k * c for k, c in zip(chunk, meta.chunks))
        ext = extent(meta, chunk)
        lo = tuple(max(a, o) for a, o in zip(start, origin))
        hi = tuple(min(b, o + e) for b, o, e in zip(stop, origin, ext))
        part = stored[tuple(slice(a - s, b - s) for a, b, s in zip(lo, hi, start))]
        key = meta.key_encoding.chunk_key(chunk)
        group, shard_key = part_of(meta, chunk)
        whole = all(a == o for a, o in zip(lo, origin)) and (
            tuple(b - a for a, b in zip(lo, hi)) == ext
        )
        if whole:
            padded = pad_to_chunk(meta, part)
            data = _encode(cfg, meta, key, padded)
            kind, empty = "enc", is_fill(padded, meta.fill_value)
        else:
            data = np.ascontiguousarray(part).tobytes()
            kind, empty = "raw", None
        yield {
            "variable": name,
            "key": key,
            "part": group,
            "shard_key": shard_key,
            "ord": 1,
            "chunk": list(chunk),
            "kind": kind,
            "lo": [a - o for a, o in zip(lo, origin)],
            "shape": list(part.shape),
            "data": data,
            "empty": empty,
            "row": None,
        }


# --- packs ---------------------------------------------------------------------------------


class _Trace:
    """Where and when one task ran and what its pack writes cost (``TaskConfig.trace``): the
    host, process, partition and attempt; wall-clock start and end; for each pack its bytes,
    the seconds its copy (mount) or put (direct) took, and which of the two it was."""

    def __init__(self):
        import socket
        import time as _time

        self._time = _time.time
        self.info = {
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "start": _time.time(),
        }
        try:
            from pyspark import TaskContext

            ctx = TaskContext.get()
            if ctx is not None:
                self.info["partition"] = ctx.partitionId()
                self.info["attempt"] = ctx.attemptNumber()
                self.info["stage"] = ctx.stageId()
        except Exception:  # noqa: BLE001 - placement is best effort
            pass
        self.packs: list[dict] = []

    def close(self, writer: PackWriter) -> None:
        start = self._time()
        writer.close()
        self.packs.append(
            {
                "bytes": writer.size,
                "s": round(self._time() - start, 4),
                "how": (
                    "put"
                    if (writer._access is not None and writer._access.is_direct)
                    else "copy"
                ),
            }
        )

    def done(self) -> dict:
        return {**self.info, "end": self._time(), "packs": self.packs}


class _Packs:
    """The pack files one task writes: one open per variable at a time, each closed (and
    a new one begun) once it holds ``roll_bytes``."""

    def __init__(self, cfg: TaskConfig, trace: "_Trace | None" = None):
        self.cfg = cfg
        self.open: dict[str, PackWriter] = {}
        self.full: list[PackWriter] = []
        self.trace = trace

    def _close(self, writer: PackWriter) -> None:
        if self.trace is None or writer._closed:
            writer.close()
        else:
            self.trace.close(writer)

    def put(self, name: str, data: bytes) -> tuple[str, int, int]:
        """Append one encoded chunk of ``name``: ``(file, offset, length)`` of its bytes."""
        cfg = self.cfg
        writer = self.open.get(name)
        if writer is None:
            writer = self.open[name] = PackWriter(
                os.path.join(cfg.root, "data", name, cfg.write_id),
                reader_root=cfg.root,
                access=cfg.access,
            )
        offset, length = writer.add(data)
        file = writer.path
        if writer.size >= cfg.roll_bytes:
            self._close(writer)
            self.full.append(writer)
            del self.open[name]
        return file, offset, length

    def finish(self) -> None:
        for w in list(self.open.values()):
            self._close(w)
        for w in self.full:
            self._close(w)

    def abort(self) -> None:
        for w in [*self.open.values(), *self.full]:
            w.abort()


def _chunk_row(
    name, key, shard_key, where, data, *, empty, rmw=False, base=None, shard_live=False
) -> dict:
    """The row of a chunk whose encoded ``data`` landed at ``where`` (``_Packs.put``)."""
    file, offset, length = where
    return _row(
        "chunk",
        kind="chunk",
        array=name,
        key=key,
        shard_key=shard_key,
        file=file,
        offset=offset,
        length=length,
        checksum=crc32c_hex(data),
        empty=empty,
        rmw=rmw,
        expected_write_id=None if base is None else base["write_id"],
        # the row replaces one for the same key: the file it recorded for the base object (a
        # registered store's own) stays the identity this chunk shadows, which is how a sync
        # tells that the base changed under it
        identity=None if base is None else base.get("identity"),
        shard_live=shard_live,
    )


# --- assemble ------------------------------------------------------------------------------


class _Group:
    """The base rows of one ``(variable, part)`` and what the write did to it."""

    def __init__(self, ident):
        self.ident = ident
        self.chunks: dict[str, dict] = {}  # base chunk rows by key
        self.shard: dict | None = None  # the base shard row
        self.rewritten: set[str] = set()
        self.read_shard = False  # a chunk was read from this group's base rows
        self.store = None


class _Assembler:
    def __init__(self, cfg: TaskConfig):
        self.cfg = cfg
        self.packs = _Packs(cfg)
        self.out: list[dict] = []
        self.group: _Group | None = None
        self.pending: list[dict] = []

    # the stream: sorted by (variable, part, ord, key)
    def feed(self, row: dict) -> None:
        if row["kind"] in ("coord", "digest"):
            self.out.append(
                _row("coord" if row["kind"] == "coord" else "digest", info=row["row"])
                | {"data": None if row["data"] is None else bytes(row["data"])}
            )
            return
        ident = (row["variable"], row["part"])
        if self.group is None or self.group.ident != ident:
            self.close_group()
            self.group = _Group(ident)
        if row["kind"] == "base":
            base = json.loads(row["row"])
            if base.get("kind") == "shard":
                self.group.shard = base
            else:
                self.group.chunks[base["key"]] = base
            return
        if self.pending and self.pending[0]["key"] != row["key"]:
            self.chunk(self.pending)
            self.pending = []
        self.pending.append(row)

    def close_group(self) -> None:
        group = self.group
        if group is None:
            return
        if self.pending:
            self.chunk(self.pending)
            self.pending = []
        name, part = group.ident
        meta = self.cfg.arrays.get(name)
        if group.read_shard and meta is not None and meta.shard is not None:
            self.out.append(
                _row(
                    "guard",
                    kind="shard",
                    array=name,
                    key=part,
                    expected_write_id=(group.shard or {}).get("write_id"),
                )
            )
        if self.cfg.mode == "overwrite":
            for key, base in sorted(group.chunks.items()):
                if key not in group.rewritten:
                    self.out.append(delete_row(base))
            if group.shard is not None:
                self.out.append(delete_row(group.shard))
        if group.store is not None:
            group.store.close()
        self.group = None

    def chunk(self, pieces: list[dict]) -> None:
        cfg, group = self.cfg, self.group
        first = pieces[0]
        name, key = first["variable"], first["key"]
        meta = cfg.arrays[name]
        chunk = tuple(first["chunk"])
        rmw = False
        if any(p["kind"] == "enc" for p in pieces):
            if len(pieces) > 1:
                raise ZarrError(
                    f"the input writes chunk {name}/{key} more than once (its blocks "
                    "overlap)",
                    fix="write each cell once: make the blocks of a variable disjoint",
                )
            data, empty = bytes(first["data"]), bool(first["empty"])
        else:
            ext = extent(meta, chunk)
            seen = np.zeros(ext, dtype=bool)
            for p in pieces:
                box = tuple(slice(a, a + n) for a, n in zip(p["lo"], p["shape"]))
                if seen[box].any():
                    raise ZarrError(
                        f"the input writes cells of chunk {name}/{key} more than once "
                        "(its blocks overlap)",
                        fix="write each cell once: make the blocks of a variable disjoint",
                    )
                seen[box] = True
            if seen.all() or cfg.mode in ("create", "overwrite"):
                values = _fill_chunk(meta)  # an overwrite replaces the variable
            else:
                values = self.base_chunk(meta, chunk, key)
                rmw = True
            native = _native(meta)
            for p in pieces:
                box = tuple(slice(a, a + n) for a, n in zip(p["lo"], p["shape"]))
                values[box] = np.frombuffer(p["data"], dtype=native).reshape(p["shape"])
            empty = is_fill(values, meta.fill_value)
            data = _encode(cfg, meta, key, values)
        group.rewritten.add(key)
        where = self.packs.put(name, data)
        self.out.append(
            _chunk_row(
                name,
                key,
                first["shard_key"],
                where,
                data,
                empty=empty,
                rmw=rmw,
                base=group.chunks.get(key),
                shard_live=group.shard is not None and cfg.mode != "overwrite",
            )
        )

    def base_chunk(self, meta: ArrayMeta, chunk, key: str) -> np.ndarray:
        """The stored values of ``chunk`` at the base, padded with fill to a whole chunk,
        read from this group's base rows (its shard row and its own chunk row)."""
        from databricks.labs.gbx.zarr._engine import open_manifest_array, read_box

        group, cfg = self.group, self.cfg
        out = _fill_chunk(meta)
        base = cfg.base_arrays.get(meta.name)
        if meta.shard is not None and key not in group.chunks:
            # the chunk comes from the shard row (or, with none, reads as fill): the commit
            # checks that row is unchanged. A chunk with its own row needs no guard: that
            # row's expectation covers it, and disjoint writers of one shard stay disjoint
            group.read_shard = True
        if base is None or (group.shard is None and key not in group.chunks):
            return out
        origin = [k * c for k, c in zip(chunk, meta.chunks)]
        box = [(o, min(o + c, n)) for o, c, n in zip(origin, meta.chunks, base.shape)]
        if any(b <= a for a, b in box):
            return out
        if group.store is None:
            from databricks.labs.gbx.zarr._access import reader_for
            from databricks.labs.gbx.zarr._store import ManifestStore

            rows = list(group.chunks.values())
            if group.shard is not None:
                rows.append(group.shard)
            group.store = ManifestStore(
                {
                    k: v
                    for k, v in cfg.base_docs.items()
                    if k.rpartition("/")[0] == meta.name
                },
                rows,
                reader_for(cfg.access, cfg.root, cfg.base_root),
                {meta.name: base},
            )
        arr = open_manifest_array(group.store, meta.name)
        values = read_box(arr, tuple(slice(a, b) for a, b in box))
        out[tuple(slice(0, b - a) for a, b in box)] = values
        return out

    def finish(self) -> None:
        self.close_group()
        self.packs.finish()

    def abort(self) -> None:
        self.packs.abort()
        if self.group is not None and self.group.store is not None:
            self.group.store.close()


def _row(rtype: str, **values) -> dict:
    return {name: values.get(name) for name in ROWS_ARROW.names} | {"rtype": rtype}


def delete_row(base: dict) -> dict:
    """The delete row (``ROWS_ARROW``) of a base manifest row, expecting its write_id."""
    return _row(
        "delete",
        kind=base.get("kind"),
        array=base.get("array"),
        key=base.get("key"),
        shard_key=base.get("shard_key"),
        identity=base.get("identity"),
        expected_write_id=base.get("write_id"),
    )


def _piece_rows(batch):
    cols = [
        "variable",
        "key",
        "part",
        "shard_key",
        "chunk",
        "kind",
        "lo",
        "shape",
        "empty",
        "row",
    ]
    rows = pa.Table.from_batches([batch]).select(cols).to_pylist()
    data = batch.column("data")
    for i, row in enumerate(rows):
        row["data"] = data[i].as_buffer()
        yield row


def assemble_function(cfg: TaskConfig):
    """Pieces and base rows to encoded chunks in pack files, and rows for the driver."""

    def run(batches):
        work = _Assembler(cfg)
        try:
            for batch in batches:
                for row in _piece_rows(batch):
                    work.feed(row)
            work.finish()
        except BaseException:
            work.abort()
            raise
        yield from _yield_rows(work.out, cfg)

    return run


# --- pack: an aligned create's one stage ----------------------------------------------------


def _yield_rows(rows: list[dict], cfg: TaskConfig):
    out, held = [], 0
    for row in rows:
        out.append(row)
        held += len(row.get("data") or b"")
        if len(out) >= cfg.batch_rows * 16 or held >= cfg.batch_bytes:
            yield _batch(out, ROWS_ARROW)
            out, held = [], 0
    if out:
        yield _batch(out, ROWS_ARROW)


def pack_function(cfg: TaskConfig):
    """Blocks straight to encoded chunks in this task's pack files (``ROWS_ARROW``), for a
    create the driver found aligned (``_write._aligned``): every piece of every block is a
    whole chunk. A piece that is not means the input changed since the position pass; the
    write stops, as its digest check would."""

    def run(batches):
        trace = _Trace() if cfg.trace else None
        packs = _Packs(cfg, trace)
        out: list[dict] = []
        seen = {"count": 0, "hash": 0, "region": {}, "variables": set()}
        try:
            for batch in batches:
                table = pa.Table.from_batches([batch])
                cols = ["variable", "dims", "start", "stop", "dtype", "shape"]
                cols += [c for c in ("attrs",) if c in table.schema.names]
                rows = table.select(cols).to_pylist()
                data = batch.column("data")
                for i, row in enumerate(rows):
                    seen["count"] += 1
                    h = block_hash(
                        row["variable"], row["start"] or (), row["stop"] or ()
                    )
                    seen["hash"] = (seen["hash"] + h) & _HASH_MASK
                    for piece in _block_pieces(cfg, row, data[i].as_buffer(), seen):
                        out.append(_packed(cfg, packs, piece))
            packs.finish()
        except BaseException:
            packs.abort()
            raise
        info = {
            "count": seen["count"],
            "hash": str(seen["hash"]),
            "region": seen["region"],
            "variables": sorted(seen["variables"]),
        }
        if trace is not None:
            info["trace"] = trace.done()
        out.append(_row("digest", info=json.dumps(info)))
        yield from _yield_rows(out, cfg)

    return run


def _packed(cfg: TaskConfig, packs: _Packs, piece: dict) -> dict:
    """The row of one piece of an aligned create: a coordinate block passes through, a whole
    chunk goes into the task's pack."""
    if piece["kind"] == "coord":
        return _row("coord", info=piece["row"]) | {"data": bytes(piece["data"])}
    name, key = piece["variable"], piece["key"]
    if piece["kind"] != "enc":
        raise ZarrError(
            f"a block of {name!r} covers chunk {key} only in part, though the position "
            "pass found every block whole: the input changed between the two passes",
            fix="the input changed between planning and writing; materialize it as a "
            "table first (write it to Delta, then pass spark.table(...))",
        )
    data = bytes(piece["data"])
    where = packs.put(name, data)
    return _chunk_row(
        name, key, piece["shard_key"], where, data, empty=bool(piece["empty"])
    )
