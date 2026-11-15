"""Write test inputs the way a Volume allows: build them on local disk, then copy the bytes over in one pass.

Under the Serverless test runner ``tmp_path`` is under the run's Volume folder (the Python workers are other
machines and read it from there). GDAL cannot update a file in place on a Volume: a GTiff whose header, mask or
overviews are written after its data, a GeoPackage (SQLite) and most other formats fail there ("Failed to create
mask."). A test that writes such a file at a ``tmp_path`` path builds it here instead.

- ``written_locally(path)`` yields a local path to write ``path`` at; on exit every file the writer made beside it
  (sidecars such as ``.aux.xml`` or ``.msk``, or a directory such as a ``.gdb``) is copied to ``path``'s folder.
- ``open_raster(fp, mode, ...)`` is ``rasterio.open`` for a write (``"w"``) or an update (``"r+"``) through
  ``written_locally``; a read, an in-memory target (``/vsi...``) or a file object goes to ``rasterio.open`` as it is.

The ``volume_like_tmp`` fixture (``test/conftest.py``) refuses a GDAL write under ``tmp_path``, as a Volume does.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import tempfile


def _local_path(fp) -> str | None:
    """``fp`` as a local file path, or None for an in-memory target or a file object."""
    try:
        path = os.fspath(fp)
    except TypeError:
        return None
    if isinstance(path, bytes):
        path = path.decode()
    return None if path.startswith("/vsi") or "://" in path else path


def _beside(path: str):
    """The entries of ``path``'s folder that belong to it: itself, and its sidecars (``<name>.*``)."""
    folder, name = os.path.split(os.path.abspath(path))
    if not os.path.isdir(folder):
        return []
    return [e for e in os.listdir(folder) if e == name or e.startswith(name + ".")]


@contextlib.contextmanager
def written_locally(path, update: bool = False):
    """A local path to write ``path`` at; what was written there is copied to ``path``'s folder on exit.

    ``update``: start from the file now at ``path`` (and its sidecars), for a writer that opens it in update mode.
    Nothing is copied when the block raises.
    """
    target = os.path.abspath(os.fspath(path))
    folder, name = os.path.split(target)
    work = tempfile.mkdtemp(prefix="gbx-local-write-")
    try:
        if update:
            for e in _beside(target):
                src = os.path.join(folder, e)
                (shutil.copytree if os.path.isdir(src) else shutil.copyfile)(
                    src, os.path.join(work, e)
                )
        yield os.path.join(work, name)
        os.makedirs(folder, exist_ok=True)
        for e in os.listdir(work):
            src, dst = os.path.join(work, e), os.path.join(folder, e)
            if os.path.isdir(src):
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                shutil.copyfile(src, dst)  # one sequential write
    finally:
        shutil.rmtree(work, ignore_errors=True)


@contextlib.contextmanager
def open_raster(fp, mode="r", *args, **kwargs):
    """``rasterio.open(fp, mode, ...)`` whose write or update is made on local disk (see the module docstring)."""
    import rasterio

    path = _local_path(fp)
    if mode not in ("w", "w+", "r+") or path is None:
        with rasterio.open(fp, mode, *args, **kwargs) as ds:
            yield ds
        return
    with written_locally(path, update=mode == "r+") as local:
        with rasterio.open(local, mode, *args, **kwargs) as ds:
            yield ds
