"""Range readers: how the store gets the bytes of a ``Region``.

``FuseRangeReader`` reads files through the local file system, which is how a
Unity Catalog Volume appears on a cluster (``/Volumes/...``). Blocking, so callers on
an event loop run it in a thread.
"""

from __future__ import annotations

import os
from typing import Protocol

from databricks.labs.gbx.zarr._shard import Region
from databricks.labs.gbx.zarr.errors import ZarrStoreDrift

__all__ = ["RangeReader", "FuseRangeReader"]


class RangeReader(Protocol):
    def read(self, regions: list[Region]) -> list[bytes]:
        """The bytes of each region, in the order given."""
        ...


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


class FuseRangeReader:
    """Reads regions from files under a root with positional ``os.pread`` calls.

    A relative ``Region.file`` resolves under ``root``, or under ``base_root`` when the
    region's ``origin`` is ``"external"`` (a registered store's own files; ``root`` then
    holds only what GeoBrix wrote). Without a ``base_root`` external files resolve
    under ``root``, and an absolute ``file`` is used as it is.

    Regions of one file that touch or overlap are fetched with one read. A file that is
    missing, or shorter than the manifest says, raises ``ZarrStoreDrift``; other I/O
    errors propagate. UC Volume FUSE mounts can raise ``FileNotFoundError`` transiently
    on a healthy file, so the open and reads of a file are retried ``attempts`` times
    (the same guard the readers use) before a missing file counts as drift.
    """

    def __init__(self, root: str, *, base_root: str | None = None, attempts: int = 10):
        self._root = root
        self._base_root = base_root
        self._attempts = attempts

    def _path(self, region: Region) -> str:
        base = (
            self._base_root
            if region.origin == "external" and self._base_root is not None
            else self._root
        )
        return os.path.join(base, region.file)

    def read(self, regions: list[Region]) -> list[bytes]:
        out: list[bytes] = [b""] * len(regions)
        by_file: dict[str, list[int]] = {}
        for i, r in enumerate(regions):
            if r.offset < 0 or r.length < 0:
                raise ValueError(f"region {r} has a negative offset or length")
            if r.length:
                by_file.setdefault(self._path(r), []).append(i)
        for path, members in by_file.items():
            spans = _coalesce(regions, members)
            for (start, _, held), data in zip(spans, self._read_spans(path, spans)):
                if len(held) == 1 and regions[held[0]].offset == start:
                    out[held[0]] = data
                    continue
                view = memoryview(data)
                for i in held:
                    r = regions[i]
                    out[i] = bytes(view[r.offset - start : r.offset - start + r.length])
        return out

    def _read_spans(self, path: str, spans) -> list[bytes]:
        from databricks.labs.gbx.ds._listing import _retry_transient

        try:
            return _retry_transient(
                lambda: self._pread_all(path, spans), attempts=self._attempts
            )
        except FileNotFoundError:
            raise ZarrStoreDrift(
                f"{path} is recorded in the manifest but missing",
                fix="run gbx_zarr_sync if the store changed outside GeoBrix; "
                "otherwise restore the file",
            ) from None

    @staticmethod
    def _pread_all(path: str, spans) -> list[bytes]:
        fd = os.open(path, os.O_RDONLY)
        try:
            out = []
            for start, stop, _ in spans:
                want, parts, at = stop - start, [], start
                while want:
                    chunk = os.pread(fd, want, at)
                    if not chunk:
                        raise ZarrStoreDrift(
                            f"{path} ends before byte {stop}, which the manifest "
                            "says it holds",
                            fix="run gbx_zarr_sync if the store changed outside "
                            "GeoBrix; otherwise the file is truncated",
                        )
                    parts.append(chunk)
                    want -= len(chunk)
                    at += len(chunk)
                out.append(parts[0] if len(parts) == 1 else b"".join(parts))
            return out
        finally:
            os.close(fd)
