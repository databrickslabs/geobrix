"""Pack files: the immutable files a write puts its encoded chunks in.

A write never modifies a file. Each task appends the chunks it encodes to a pack, one
pack per array, staged on local disk and then copied to the store with one sequential
``shutil.copyfile`` (a Unity Catalog Volume is a FUSE mount: no seeks, no appends):
``<root>/data/<array>/<write_id>/part-<uuid>.zpack``. A manifest row then points at each
chunk as ``(file, offset, length)``, ``file`` relative to the root. With direct access the
pack is put to the Volume's cloud storage with one ``obstore.put`` instead (the object
appears whole or not at all), at the same path.

``encode_inner_chunk`` encodes one inner chunk exactly as zarr would store it, through a
``zarr`` array over a ``MemoryStore`` with the array's own codecs, so the bytes are a
standard chunk that zarr (and export, without decoding) reads as is.
"""

from __future__ import annotations

import copy
import json
import os
import shutil
import threading
import uuid
from collections import OrderedDict

import numpy as np

from databricks.labs.gbx.core._retry import _retry_transient
from databricks.labs.gbx.zarr._access import put_pack, stat_for
from databricks.labs.gbx.zarr._meta import ArrayMeta, inner_meta_doc
from databricks.labs.gbx.zarr.errors import ZarrError

__all__ = [
    "PackWriter",
    "encode_inner_chunk",
    "crc32c_hex",
    "is_fill",
    "pad_to_chunk",
    "require_packs",
]

_ENCODERS: OrderedDict[str, tuple] = (
    OrderedDict()
)  # document text -> (array, store, key)
_ENCODERS_LOCK = threading.Lock()
_ENCODERS_KEPT = 16


def _encoder_docs(meta: ArrayMeta) -> dict[str, bytes]:
    """The documents of a one-chunk array whose single chunk is one of ``meta``'s inner chunks."""
    if meta.zarr_format == 3:
        return {"zarr.json": json.dumps(inner_meta_doc(meta), sort_keys=True).encode()}
    zarray = copy.deepcopy(meta.doc[".zarray"])
    zarray["shape"] = list(meta.chunks)
    zarray["chunks"] = list(meta.chunks)
    return {".zarray": json.dumps(zarray, sort_keys=True).encode()}


def _encoder(meta: ArrayMeta):
    """A cached ``(array, held, key)``: writing a block to ``array`` stores its bytes in
    ``held[key]``. One per distinct document, so a task opens each array once."""
    import zarr
    from zarr.core.buffer import default_buffer_prototype
    from zarr.storage import MemoryStore

    docs = _encoder_docs(meta)
    text = json.dumps({k: v.decode() for k, v in docs.items()}, sort_keys=True) + str(
        meta.zarr_format
    )
    with _ENCODERS_LOCK:
        hit = _ENCODERS.get(text)
        if hit is not None:
            _ENCODERS.move_to_end(text)
            return hit
    proto = default_buffer_prototype()
    held = {k: proto.buffer.from_bytes(v) for k, v in docs.items()}
    store = MemoryStore(store_dict=held)
    arr = zarr.open_array(
        store=store, mode="r+", zarr_format=meta.zarr_format
    ).with_config({"write_empty_chunks": True})
    key = meta.key_encoding.chunk_key((0,) * len(meta.chunks))
    hit = (arr, held, key, threading.Lock())
    with _ENCODERS_LOCK:
        _ENCODERS[text] = hit
        while len(_ENCODERS) > _ENCODERS_KEPT:
            _ENCODERS.popitem(last=False)
    return hit


def pad_to_chunk(meta: ArrayMeta, block: np.ndarray) -> np.ndarray:
    """``block`` (an edge chunk may be smaller) padded with the fill value to a whole chunk."""
    block = np.asarray(block)
    if tuple(block.shape) == tuple(meta.chunks):
        return block
    if block.ndim != len(meta.chunks) or any(
        b > c for b, c in zip(block.shape, meta.chunks)
    ):
        raise ValueError(
            f"a block of shape {block.shape} does not fit a chunk of {meta.chunks}"
        )
    fill = meta.fill_value
    native = np.dtype(meta.dtype).newbyteorder("=")
    out = np.full(meta.chunks, 0 if fill is None else fill, dtype=native)
    out[tuple(slice(0, n) for n in block.shape)] = block
    return out


def encode_inner_chunk(meta: ArrayMeta, block: np.ndarray) -> bytes:
    """The stored bytes of one inner chunk of ``meta`` holding ``block``.

    ``block`` holds stored values (``meta.dtype``, already CF-encoded). An edge chunk may
    be smaller than the chunk shape: it is padded with the fill value, as zarr does. For a
    sharded array these are the bytes of one inner chunk, encoded with the shard's inner
    codecs; for an unsharded one, the bytes of the chunk object.
    """
    padded = pad_to_chunk(meta, block)
    arr, held, key, lock = _encoder(meta)
    with lock:
        arr[...] = padded
        data = held[key].to_bytes()
        del held[key]
    return data


def crc32c_hex(data) -> str:
    """The CRC-32C of ``data`` as eight lowercase hex digits."""
    from numcodecs import CRC32C

    return f"{CRC32C.checksum(bytes(data)) & 0xFFFFFFFF:08x}"


def is_fill(chunk: np.ndarray, fill) -> bool:
    """Whether every cell of ``chunk`` is the fill value (NaN matches NaN)."""
    if fill is None:
        return False
    chunk = np.asarray(chunk)
    if chunk.dtype.kind in "fc" and isinstance(fill, (float, complex)):
        if np.isnan(fill):
            return bool(np.isnan(chunk).all())
    return bool((chunk == fill).all())


def require_packs(root: str, files, *, what: str, access=None) -> None:
    """Raise ``ZarrError`` unless every pack file in ``files`` (paths relative to ``root``)
    still exists. With direct ``access`` (an ``AccessConfig``) the objects are looked up in
    the cloud storage, where a pack just put is visible at once; the mount may lag.

    A commit calls this just before it points rows at the packs its files phase wrote: a
    vacuum forced to delete files younger than the store's retain (``force=True``) can have
    removed them meanwhile, and a row that points at a missing file is a version that
    cannot be read. ``what`` names the operation in the message.
    """
    gone = []
    stat = stat_for(access)
    for f in sorted(set(files)):
        path = f if os.path.isabs(f) else os.path.join(root, f)
        try:
            _retry_transient(lambda p=path: stat(p), attempts=3, backoff=0.2)
        except FileNotFoundError:
            gone.append(f)
    if gone:
        raise ZarrError(
            f"{len(gone)} pack file(s) of this {what} are gone before it could commit "
            f"(e.g. {gone[0]}): a vacuum with force=True removed the files of a write "
            "that was still running",
            fix=f"run the {what} again; vacuum without force=True (the store's own "
            "retain) while writes are running",
        )


class PackWriter:
    """Appends encoded chunks to one new pack file of a write.

    ``dest_dir`` is ``<root>/data/<array>/<write_id>`` and ``reader_root`` the store root
    that manifest ``file`` values are relative to. ``add`` returns where the bytes landed
    in the pack; ``close`` copies the pack to ``dest_dir`` (or, with a direct ``access``,
    puts it there in the cloud storage) and returns its path relative to ``reader_root``
    (``None`` when nothing was added: no file is made). The pack is staged on local disk
    until then, so a task holds only the chunk it is encoding.
    """

    def __init__(self, dest_dir: str, *, reader_root: str, access=None):
        self.dest_dir = dest_dir
        self.reader_root = reader_root
        self._access = access
        self.name = f"part-{uuid.uuid4().hex}.zpack"
        self.size = 0
        self._local: str | None = None
        self._file = None
        self._closed = False
        self._result: str | None = None

    @property
    def path(self) -> str:
        """The pack's path relative to ``reader_root`` (what the manifest records)."""
        rel = os.path.relpath(os.path.join(self.dest_dir, self.name), self.reader_root)
        return rel.replace(os.sep, "/")

    def add(self, data) -> tuple[int, int]:
        if self._closed:
            raise ValueError("this pack is closed")
        if self._file is None:
            from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_file

            self._local = new_local_temp_file(suffix=".zpack", prefix="gbx_zarr_")
            self._file = open(self._local, "wb")
        view = memoryview(data)
        offset = self.size
        self._file.write(view)
        self.size += view.nbytes
        return offset, view.nbytes

    def close(self) -> str | None:
        if self._closed:
            return self._result
        self._closed = True
        if self._file is None:
            return None
        self._file.close()
        dest = os.path.join(self.dest_dir, self.name)
        try:
            put = self._access is not None and self._access.is_direct
            # False: the storage refused the credentials and the call is "auto": the mount
            if not (
                put
                and _retry_transient(lambda: put_pack(self._access, self._local, dest))
            ):
                _retry_transient(lambda: os.makedirs(self.dest_dir, exist_ok=True))
                _retry_transient(lambda: shutil.copyfile(self._local, dest))
        except OSError as e:
            raise ZarrError(
                f"could not write the pack file {dest} ({e})",
                fix="check that the store root is on a Volume you can write to, then "
                "write again",
            ) from e
        finally:
            self._discard_local()
        self._result = self.path
        return self._result

    def abort(self) -> None:
        """Drop the staged pack without copying it (nothing reaches the store)."""
        self._closed = True
        if self._file is not None:
            self._file.close()
        self._discard_local()

    def _discard_local(self) -> None:
        if self._local is not None:
            try:
                os.remove(self._local)
            except OSError:
                pass
            self._local = None
