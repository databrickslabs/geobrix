"""Engines: how the Zarr readers open arrays and read boxes of values from them.

Two kinds of array, two openers, one reader:

- ``open_manifest_array`` opens an array of a ``ManifestStore``: by default through the
  native engine (``_native.py``), which batches the byte ranges of a box and decodes the
  chunks on a thread pool; or as a read-only ``zarr.Array`` read by zarr-python.
- ``open_plain_array`` opens an array of an ordinary store on a local or FUSE path
  (a Unity Catalog Volume) through tensorstore, or through zarr-python.
- ``read_box`` reads a box of either kind as a ``numpy`` array.

The zarrs codec pipeline is a Rust decoder for zarr-python. zarr picks the pipeline when
an array is opened, and ``codec_pipeline.path`` is a setting of the whole process, so the
engines select zarrs only inside a short ``zarr.config.set`` scope around the open, under
a lock, and never leave it set: other code in the process keeps zarr's default. zarrs-python
reads only the stores it knows (local files, object stores, HTTP) and has no way to take a
``ManifestStore``; zarr then quietly uses its own pipeline. So asking for
``pipeline="zarrs"`` on a manifest array is an error rather than a silent no-op, and the
zarrs pipeline only ever decodes plain stores on a local path, which is where
``engine="zarr"`` uses it. (Performance gate G1 found zarr-python over a ``ManifestStore``
many times slower than tensorstore on the same shards, because it opens a file per
chunk; the native engine is the answer.)

``tensorstore`` is imported only when it is used, so importing this module stays light.
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager

import numpy as np
import zarr

from databricks.labs.gbx.zarr.errors import ZarrError, ZarrUnsupported

__all__ = ["open_manifest_array", "open_plain_array", "read_box"]

_ZARRS_PIPELINE = "zarrs.ZarrsCodecPipeline"
_PIPELINE_KEY = "codec_pipeline.path"
_MANIFEST_PIPELINES = ("auto", "native", "zarrs", "default")
_PLAIN_ENGINES = ("auto", "tensorstore", "zarr")
_PLAIN_PIPELINES = ("auto", "zarrs", "default")
# tensorstore: concurrent file reads. A FUSE read costs 40-50 ms of latency, so many
# in flight pay off (the spike's setting).
_TENSORSTORE_FILE_IO = 64
_INSTALL_FIX = (
    "install the Zarr extra (geobrix[zarr_dbr19] on DBR 19, geobrix[zarr_env6] on "
    "Serverless environment 6)"
)

# zarr.config is process-wide state; two scopes must not overlap or the later one would
# restore the earlier one's setting. Reentrant: a scope may open another array inside.
_CONFIG_LOCK = threading.RLock()


@contextmanager
def _zarrs_scope():
    with _CONFIG_LOCK, zarr.config.set({_PIPELINE_KEY: _ZARRS_PIPELINE}):
        yield


def _zarrs_importable() -> bool:
    try:
        import zarrs  # noqa: F401
    except ImportError:
        return False
    return True


def _pipeline_of(arr: zarr.Array) -> str:
    """``"zarrs"`` when the array decodes through zarrs, else ``"default"``."""
    pipeline = getattr(getattr(arr, "_async_array", None), "codec_pipeline", None)
    engaged = (
        type(pipeline).__name__ == "ZarrsCodecPipeline"
        and getattr(pipeline, "impl", None) is not None
    )
    return "zarrs" if engaged else "default"


# --- manifest arrays ------------------------------------------------------------


def open_manifest_array(store, array: str, *, pipeline: str = "auto"):
    """The array ``array`` of a ``ManifestStore``, for ``read_box``.

    ``pipeline`` picks how the bytes are read and decoded:

    - ``"native"``: batched byte ranges decoded on the store's thread pool
      (``_native.py``); returns an array object with ``shape``, ``dtype`` and ``read``.
      Raises ``ZarrUnsupported`` for codecs it does not decode.
    - ``"default"``: zarr-python's own codec pipeline; returns a read-only ``zarr.Array``.
    - ``"zarrs"``: the zarrs pipeline. zarrs-python cannot read a ``ManifestStore``, so
      this raises ``ZarrUnsupported`` instead of letting zarr quietly fall back.
    - ``"auto"``: ``"native"``, or ``"default"`` for an array native does not decode.
    """
    if pipeline not in _MANIFEST_PIPELINES:
        raise ValueError(
            f"pipeline must be one of {_MANIFEST_PIPELINES}, got {pipeline!r}"
        )
    if pipeline == "auto":
        try:
            return _open_native(store, array)
        except ZarrUnsupported:
            return _open_in_store(store, array)
    if pipeline == "native":
        return _open_native(store, array)
    if pipeline == "default":
        return _open_in_store(store, array)
    if not _zarrs_importable():
        raise ZarrUnsupported(
            "the zarrs pipeline needs the zarrs package, which is not installed",
            fix=_INSTALL_FIX + ", or use pipeline='default'",
        )
    with _zarrs_scope():
        arr = _open_in_store(store, array)
    if _pipeline_of(arr) != "zarrs":
        raise ZarrUnsupported(
            f"the zarrs pipeline cannot read a {type(store).__name__}: zarrs-python "
            "supports local, object and HTTP stores only",
            fix="open the array with pipeline='default' (or 'auto')",
        )
    return arr


def _no_array(store, array: str) -> ZarrError:
    known = sorted(getattr(store, "_arrays", {}))
    return ZarrError(
        f"the store has no array {array!r}",
        fix="use one of its arrays" + (f": {', '.join(known)}" if known else ""),
    )


def _open_native(store, array: str):
    meta = getattr(store, "_arrays", {}).get(array)
    if meta is None:
        raise _no_array(store, array)
    from databricks.labs.gbx.zarr._native import make_decoder

    return _NativeManifestArray(store, meta, make_decoder(meta))


def _open_in_store(store, array: str) -> zarr.Array:
    try:
        return zarr.open_array(store=store, path=array, mode="r")
    except (FileNotFoundError, KeyError, zarr.errors.NodeNotFoundError) as e:
        raise _no_array(store, array) from e


# --- plain stores ---------------------------------------------------------------


class PlainArray:
    """A read-only array of a plain store, read through ``engine``.

    ``shape`` and ``dtype`` describe the array; ``engine`` is ``"tensorstore"`` or
    ``"zarr"``; ``pipeline`` is ``"zarrs"`` or ``"default"`` for the zarr engine and
    ``None`` for tensorstore. ``read`` returns the box as a ``numpy`` array.
    """

    shape: tuple[int, ...]
    dtype: np.dtype
    engine: str
    pipeline: str | None

    def read(self, slices) -> np.ndarray:
        box = _box(self.shape, slices)
        return np.asarray(self._read(box))

    def _read(self, box: list[tuple[int, int]]) -> np.ndarray:
        raise NotImplementedError


class _ZarrPlainArray(PlainArray):
    engine = "zarr"

    def __init__(self, arr: zarr.Array):
        self._arr = arr
        self.shape = tuple(arr.shape)
        self.dtype = np.dtype(arr.dtype)
        self.pipeline = _pipeline_of(arr)

    def _read(self, box):
        return self._arr[tuple(slice(a, b) for a, b in box)]


class _TensorstorePlainArray(PlainArray):
    engine = "tensorstore"
    pipeline = None

    def __init__(self, store):
        self._store = store
        self.shape = tuple(int(n) for n in store.shape)
        self.dtype = np.dtype(store.dtype.numpy_dtype)

    def _read(self, box):
        return self._store[tuple(slice(a, b) for a, b in box)].read().result()


class _NativeManifestArray(PlainArray):
    engine = "manifest"
    pipeline = "native"

    def __init__(self, store, meta, decode):
        self._store, self._meta, self._decode = store, meta, decode
        self.shape = tuple(meta.shape)
        self.dtype = np.dtype(meta.dtype)

    def _read(self, box):
        from databricks.labs.gbx.zarr._native import read_box_native

        return read_box_native(self._store, self._meta, box, self._decode)


def open_plain_array(
    path: str, array: str, *, engine: str = "auto", pipeline: str = "auto"
) -> PlainArray:
    """The array ``array`` of the plain Zarr store at ``path`` (a local or FUSE path).

    ``engine`` is ``"tensorstore"``, ``"zarr"`` (zarr-python) or ``"auto"`` (tensorstore,
    falling back to zarr when it is not installed). Tensorstore was the fastest reader of
    a whole array on a Volume, but regressed on the small per-task tile reads of a Spark
    job (spike read results): those pass ``engine="zarr"``.

    ``pipeline`` is how the zarr engine decodes: ``"zarrs"``, ``"default"`` (zarr's own)
    or ``"auto"`` (zarrs when it is installed). Naming one makes the automatic engine
    zarr; with ``engine="tensorstore"`` it is an error. Gate G1 measured zarrs slower than
    zarr's own pipeline for box reads over a Volume, so a reader that depends on it
    should measure.
    """
    if engine not in _PLAIN_ENGINES:
        raise ValueError(f"engine must be one of {_PLAIN_ENGINES}, got {engine!r}")
    if pipeline not in _PLAIN_PIPELINES:
        raise ValueError(
            f"pipeline must be one of {_PLAIN_PIPELINES}, got {pipeline!r}"
        )
    if pipeline != "auto" and engine == "tensorstore":
        raise ValueError(
            f"pipeline={pipeline!r} applies to the zarr engine, not engine='tensorstore'"
        )
    root = os.path.join(path, array)
    if engine == "auto":
        use_ts = pipeline == "auto" and _tensorstore_importable()
        engine = "tensorstore" if use_ts else "zarr"
    if engine == "tensorstore":
        return _open_tensorstore(root, path, array)
    return _open_zarr(root, path, array, pipeline)


def _tensorstore_importable() -> bool:
    try:
        import tensorstore  # noqa: F401
    except ImportError:
        return False
    return True


def _missing(path: str, array: str) -> ZarrError:
    return ZarrError(
        f"the store at {path} has no array {array!r}",
        fix="check the array name and that the path is a Zarr store",
    )


def _open_tensorstore(root: str, path: str, array: str) -> PlainArray:
    try:
        import tensorstore as ts
    except ImportError:
        raise ZarrUnsupported(
            "the tensorstore engine needs the tensorstore package, which is not installed",
            fix=_INSTALL_FIX + ", or use engine='zarr'",
        ) from None
    if os.path.exists(os.path.join(root, "zarr.json")):
        driver = "zarr3"
    elif os.path.exists(os.path.join(root, ".zarray")):
        driver = "zarr"
    else:
        raise _missing(path, array)
    spec = {"driver": driver, "kvstore": {"driver": "file", "path": root + "/"}}
    context = ts.Context({"file_io_concurrency": {"limit": _TENSORSTORE_FILE_IO}})
    return _TensorstorePlainArray(
        ts.open(spec, context=context, read=True, write=False).result()
    )


def _open_zarr(root: str, path: str, array: str, pipeline: str) -> PlainArray:
    use_zarrs = pipeline == "zarrs" or (pipeline == "auto" and _zarrs_importable())
    if pipeline == "zarrs" and not _zarrs_importable():
        raise ZarrUnsupported(
            "the zarrs pipeline needs the zarrs package, which is not installed",
            fix=_INSTALL_FIX + ", or use pipeline='default'",
        )
    try:
        if use_zarrs:
            with _zarrs_scope():
                arr = zarr.open_array(root, mode="r")
        else:
            arr = zarr.open_array(root, mode="r")
    except (FileNotFoundError, KeyError, zarr.errors.NodeNotFoundError) as e:
        raise _missing(path, array) from e
    return _ZarrPlainArray(arr)


# --- reading a box --------------------------------------------------------------


def _box(shape: tuple[int, ...], slices) -> list[tuple[int, int]]:
    """``(start, stop)`` per axis of ``slices`` clamped to ``shape`` as numpy would.

    Fewer slices than axes leave the later axes whole.
    """
    slices = tuple(slices)
    if len(slices) > len(shape):
        raise ValueError(
            f"read_box got {len(slices)} slices for an array of {len(shape)} axes"
        )
    box = []
    for axis, n in enumerate(shape):
        s = slices[axis] if axis < len(slices) else slice(None)
        if not isinstance(s, slice):
            raise ValueError(f"read_box takes slices; axis {axis} got {s!r}")
        start, stop, step = s.indices(n)
        if step != 1:
            raise ValueError(f"read_box takes a step of 1; axis {axis} got {step}")
        box.append((start, max(start, stop)))
    return box


def read_box(arr, slices) -> np.ndarray:
    """The box ``slices`` of ``arr`` (an array from either opener) as a ``numpy`` array.

    ``slices`` holds one ``slice`` per leading axis, with a step of 1; a missing trailing
    axis is read whole and bounds are clamped as numpy does.
    """
    if isinstance(arr, PlainArray):
        return arr.read(slices)
    if isinstance(arr, zarr.Array):
        box = _box(tuple(arr.shape), slices)
        return np.asarray(arr[tuple(slice(a, b) for a, b in box)])
    raise TypeError(
        f"read_box reads arrays from open_manifest_array or open_plain_array, not "
        f"{type(arr).__name__}"
    )
