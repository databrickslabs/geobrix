"""Range readers: how the store gets the bytes of a ``Region``.

``FuseRangeReader`` reads files through the local file system, which is how a
Unity Catalog Volume appears on a cluster (``/Volumes/...``). Blocking, so callers on
an event loop run it in a thread. It opens only files under the store's roots: a
manifest is data, and a row must not be able to name ``/etc/passwd``. The name is checked
before the file is opened and, on Linux, the opened descriptor is checked again.
"""

from __future__ import annotations

import os
import sys
from typing import Protocol

from databricks.labs.gbx.core._retry import _retry_transient
from databricks.labs.gbx.zarr._shard import Region
from databricks.labs.gbx.zarr.errors import ZarrStoreDrift

__all__ = ["RangeReader", "FuseRangeReader"]

# A transient FUSE miss is worth another try; these will not change by waiting.
_PERMANENT = (PermissionError, IsADirectoryError, NotADirectoryError)
_SYNC_FIX = "run gbx_zarr_sync if the store changed outside GeoBrix; otherwise "
_PROC_FD = "/proc/self/fd"
# Where an open descriptor can be traced back to the file it holds (Linux).
_CHECK_DESCRIPTORS = sys.platform.startswith("linux") and os.path.isdir(_PROC_FD)


class RangeReader(Protocol):
    def read(self, regions: list[Region]) -> list[bytes]:
        """The bytes of each region, in the order given."""
        ...


class _Permanent(Exception):
    """Carries a permanent OS error through ``_retry_transient`` without a retry."""


def _coalesce(
    regions: list[Region], members: list[int]
) -> list[tuple[int, int, list[int]]]:
    """Merge the regions at ``members`` (all of one file) into ``(start, stop, members)``
    spans: ranges that touch or overlap share one read."""
    spans: list[tuple[int, int, list[int]]] = []
    for i in sorted(members, key=lambda i: regions[i].offset):
        r = regions[i]
        if spans and r.offset <= spans[-1][1]:
            start, stop, held = spans[-1]
            spans[-1] = (start, max(stop, r.offset + r.length), held + [i])
        else:
            spans.append((r.offset, r.offset + r.length, [i]))
    return spans


def _held_path(fd: int) -> str:
    """The path of the file an open descriptor holds, as the kernel reports it.

    The link target is already fully resolved, so it is read with one ``readlink`` rather
    than walked with ``realpath`` (an ``lstat`` per path component, each a round trip on a
    FUSE mount).
    """
    return os.readlink(f"{_PROC_FD}/{fd}")


def _within(path: str, root: str) -> bool:
    """``path`` is ``root`` or below it (a whole-component test, not a prefix test)."""
    return os.path.commonpath([path, root]) == root


class FuseRangeReader:
    """Reads regions from files under a root with positional ``os.pread`` calls.

    A relative ``Region.file`` resolves under ``root``, or under ``base_root`` when the
    region's ``origin`` is ``"external"`` (a registered store's own files; ``root`` then
    holds only what GeoBrix wrote), and must stay under that directory. An absolute
    ``file`` must lie under ``root`` or ``base_root``. A ``..`` that leaves the root, a
    symlink that leads out of it, an empty or non-string ``file`` raise
    ``ZarrStoreDrift`` before anything is opened. Without a ``base_root`` external
    files resolve under ``root``.

    Regions of one file that touch or overlap are fetched with one read. A file that is
    missing, or shorter than the manifest says, raises ``ZarrStoreDrift``; other I/O
    errors propagate. UC Volume FUSE mounts can raise ``FileNotFoundError`` transiently
    on a healthy file, so the open and reads of a file are retried ``attempts`` times
    (the same guard the readers use) before a missing file counts as drift. Errors that
    waiting cannot fix are raised at once: a path that names a directory is drift, and a
    permission error propagates.

    The name check can lag the file system: the real path of a name is looked up once
    and kept, and a link can be put in the name's place between the check and the open.
    So on Linux the opened descriptor is traced (``/proc/self/fd``) to the file it really
    holds, which must lie under the same real root; if it does not the descriptor is
    closed and ``ZarrStoreDrift`` is raised before a byte is read.
    """

    def __init__(self, root: str, *, base_root: str | None = None, attempts: int = 10):
        self._root = root
        self._base_root = base_root
        self._attempts = attempts
        self._real: dict[str, str] = {}  # path -> os.path.realpath(path)

    def _realpath(self, path: str) -> str:
        real = self._real.get(path)
        if real is None:
            real = self._real[path] = os.path.realpath(path)
        return real

    def _resolve(self, region: Region) -> tuple[str, str]:
        """``(path to open, real path of the root it lies under)`` for ``region``, or
        ``ZarrStoreDrift`` if it leaves the roots."""
        file = region.file
        if not isinstance(file, str) or not file or "\0" in file:
            raise ZarrStoreDrift(
                f"manifest file {file!r} is not a usable path",
                fix="correct the row or run gbx_zarr_sync",
            )
        external = region.origin == "external" and self._base_root is not None
        base = os.path.normpath(
            os.path.abspath(self._base_root if external else self._root)
        )
        if os.path.isabs(file):
            cand = os.path.normpath(file)
            roots = [
                os.path.normpath(os.path.abspath(r))
                for r in (self._root, self._base_root)
                if r
            ]
        else:
            cand = os.path.normpath(os.path.join(base, file))
            roots = [base]
        for root in roots:
            real_root = self._realpath(root)
            if _within(cand, root) and _within(self._realpath(cand), real_root):
                return cand, real_root
        raise ZarrStoreDrift(
            f"manifest file {file!r} resolves outside the store root {' or '.join(roots)}",
            fix="correct the row or run gbx_zarr_sync; a manifest may only point at "
            "files under the store root or, for a registered store, its base root",
        )

    def read(self, regions: list[Region]) -> list[bytes]:
        out: list[bytes] = [b""] * len(regions)
        by_file: dict[str, list[int]] = {}
        root_of: dict[str, str] = (
            {}
        )  # path -> real path of the root it was checked under
        for i, r in enumerate(regions):
            path, root_of[path] = self._resolve(r)
            if r.offset < 0 or r.length < 0:
                raise ZarrStoreDrift(
                    f"manifest region {r.length} bytes at offset {r.offset} of "
                    f"{r.file!r} is negative",
                    fix="correct the row or run gbx_zarr_sync",
                )
            if r.length:
                by_file.setdefault(path, []).append(i)
        for path, members in by_file.items():
            spans = _coalesce(regions, members)
            for (start, _, held), data in zip(
                spans, self._read_spans(path, spans, root_of[path])
            ):
                if len(held) == 1 and regions[held[0]].offset == start:
                    out[held[0]] = data
                    continue
                view = memoryview(data)
                for i in held:
                    r = regions[i]
                    out[i] = bytes(view[r.offset - start : r.offset - start + r.length])
        return out

    def _read_spans(self, path: str, spans, real_root: str) -> list[bytes]:
        def attempt() -> list[bytes]:
            try:
                return self._pread_all(path, spans, real_root)
            except _PERMANENT as e:
                raise _Permanent(e) from None

        try:
            return _retry_transient(attempt, attempts=self._attempts)
        except _Permanent as p:
            if isinstance(p.args[0], IsADirectoryError):
                raise ZarrStoreDrift(
                    f"{path} is recorded in the manifest as a file but is a directory",
                    fix=_SYNC_FIX + "restore the file",
                ) from None
            raise p.args[0] from None
        except FileNotFoundError:
            raise ZarrStoreDrift(
                f"{path} is recorded in the manifest but missing",
                fix=_SYNC_FIX + "restore the file",
            ) from None

    @staticmethod
    def _pread_all(path: str, spans, real_root: str) -> list[bytes]:
        fd = os.open(path, os.O_RDONLY)
        try:
            if _CHECK_DESCRIPTORS:
                held = _held_path(fd)
                # an absolute path is the file the descriptor really holds
                if os.path.isabs(held) and not _within(held, real_root):
                    raise ZarrStoreDrift(
                        f"manifest file {path!r} resolves outside the store root "
                        f"{real_root} (it opened {held!r})",
                        fix="correct the row or run gbx_zarr_sync; a manifest may "
                        "only point at files under the store root or, for a "
                        "registered store, its base root",
                    )
            out = []
            for start, stop, _ in spans:
                want, parts, at = stop - start, [], start
                while want:
                    chunk = os.pread(fd, want, at)
                    if not chunk:
                        raise ZarrStoreDrift(
                            f"{path} ends before byte {stop}, which the manifest "
                            "says it holds",
                            fix=_SYNC_FIX + "the file is truncated",
                        )
                    parts.append(chunk)
                    want -= len(chunk)
                    at += len(chunk)
                out.append(parts[0] if len(parts) == 1 else b"".join(parts))
            return out
        finally:
            os.close(fd)
