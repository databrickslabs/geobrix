"""Read part of a cloud-optimized GeoTIFF from fetched byte ranges.

A Unity Catalog function cannot open a path, but SQL can fetch byte ranges of a FILE value. This module plans
which ranges a clip needs, from the TIFF header, and opens the file over only those bytes. A read outside the
fetched bytes raises an error: it never returns zeros.
"""

from __future__ import annotations

import bisect
import io
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, List, Optional, Sequence, Tuple

HEAD_BYTES = 4 * 1024 * 1024
_NAME = "gbx_ranges.tif"


class MissingRangeError(IOError):
    """GDAL asked for bytes that were not fetched."""


class HeaderTooLargeError(ValueError):
    """The TIFF header (IFDs and block tables) does not fit in the fetched head."""


class NotTiledError(ValueError):
    """A striped GeoTIFF: a window cannot be read by blocks."""


@dataclass(frozen=True)
class Plan:
    overview_level: Optional[int]
    window: Optional[Tuple[int, int, int, int]]
    ranges: Tuple[Tuple[int, int], ...]


def _merge(pieces: Sequence[Tuple[int, bytes]]) -> List[Tuple[int, bytes]]:
    """Non-overlapping, sorted spans; overlapping or touching pieces join."""
    out: List[Tuple[int, bytearray]] = []
    for off, data in sorted(
        ((int(o), bytes(d)) for o, d in pieces if d), key=lambda p: p[0]
    ):
        if out and off <= out[-1][0] + len(out[-1][1]):
            o0, buf = out[-1]
            end = off + len(data)
            if end > o0 + len(buf):
                buf.extend(data[o0 + len(buf) - off :])
        else:
            out.append((off, bytearray(data)))
    return [(o, bytes(b)) for o, b in out]


class _Ranges(io.RawIOBase):
    def __init__(
        self,
        size: int,
        spans: List[Tuple[int, bytes]],
        misses: Optional[List[Tuple[int, int]]] = None,
    ):
        self._size, self._spans, self._pos = int(size), spans, 0
        self._starts = [o for o, _ in spans]
        self._misses = misses if misses is not None else []

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._pos

    def seek(self, offset, whence=io.SEEK_SET):
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self._pos, io.SEEK_END: self._size}[whence]
        self._pos = max(0, base + int(offset))
        return self._pos

    def readinto(self, buf):
        want = min(len(buf), self._size - self._pos)
        if want <= 0:
            return 0
        got = 0
        while got < want:
            at = self._pos + got
            i = bisect.bisect_right(self._starts, at) - 1
            if i < 0 or at >= self._spans[i][0] + len(self._spans[i][1]):
                # Record the miss and stop with a short read. No Python exception may escape into GDAL's read
                # callback (it surfaces as an "unraisable" warning); the short read makes GDAL fail the read, so
                # missing bytes never come back as zeros.
                self._misses.append((at, self._pos + want - at))
                break
            off, data = self._spans[i]
            take = min(want - got, off + len(data) - at)
            buf[got : got + take] = data[at - off : at - off + take]
            got += take
        self._pos += got
        return got


class _Opener:
    """The file-system object rasterio's ``opener=`` takes. ``open`` returns a fresh file object per call.

    ``misses`` lists every (offset, length) a read asked for outside the fetched bytes, across all files opened.
    """

    def __init__(self, size: int, pieces: Sequence[Tuple[int, bytes]]):
        self._size, self._spans = int(size), _merge(pieces)
        self.misses: List[Tuple[int, int]] = []

    def open(self, path, mode="rb", **kw):
        if not str(path).endswith(_NAME):
            raise FileNotFoundError(path)
        return _Ranges(self._size, self._spans, self.misses)

    def isfile(self, path):
        return str(path).endswith(_NAME)

    def isdir(self, path):
        return False

    def ls(self, path, detail=False, **kw):
        return []

    def mtime(self, path):
        return 0

    def size(self, path):
        return self._size

    def info(self, path):
        return {"size": self._size, "type": "file"}


@contextmanager
def _open(opener: _Opener, overview_level: Optional[int] = None) -> Iterator:
    import rasterio

    kw = {} if overview_level is None else {"overview_level": int(overview_level)}
    with rasterio.open(_NAME, driver="GTiff", opener=opener, **kw) as ds:
        yield ds


@contextmanager
def open_ranges(
    size: int,
    pieces: Sequence[Tuple[int, bytes]],
    overview_level: Optional[int] = None,
    misses: Optional[List[Tuple[int, int]]] = None,
) -> Iterator:
    """The COG opened over only ``pieces`` (offset, bytes); GTiff driver only.

    A read outside ``pieces`` fails (GDAL raises a rasterio error); it never returns zeros. Pass a list as
    ``misses`` to receive each (offset, length) that was asked for and not fetched.
    """
    opener = _Opener(size, pieces)
    if misses is not None:
        opener.misses = misses
    with _open(opener, overview_level) as ds:
        yield ds


def pick_overview(ds, resolution: Optional[float]) -> Optional[int]:
    """The coarsest overview whose pixel is still at most ``resolution`` (source CRS units); None = full resolution."""
    if resolution is None:
        return None
    native = abs(ds.res[0])
    factors = ds.overviews(1)
    best = None
    for k, f in enumerate(factors):
        if native * f <= float(resolution) + 1e-9:
            best = k
    if best is None and factors and native * factors[-1] < float(resolution):
        best = len(factors) - 1
    return best


def _block_ranges(
    ds, window: Tuple[int, int, int, int], misses: List[Tuple[int, int]]
) -> List[Tuple[int, int]]:
    from rasterio.enums import Interleaving

    col_off, row_off, w, h = window
    bh, bw = ds.block_shapes[0]
    bands = range(1, ds.count + 1) if ds.interleaving == Interleaving.band else (1,)
    out = []
    for b in bands:
        for by in range(row_off // bh, (row_off + h - 1) // bh + 1):
            for bx in range(col_off // bw, (col_off + w - 1) // bw + 1):
                seen = len(misses)
                off = ds.get_tag_item(f"BLOCK_OFFSET_{bx}_{by}", "TIFF", bidx=b)
                n = ds.get_tag_item(f"BLOCK_SIZE_{bx}_{by}", "TIFF", bidx=b)
                if len(misses) > seen:
                    # GDAL loads the tile offset and size arrays lazily: they lie past the fetched head.
                    raise MissingRangeError(
                        f"the block table was not fetched (bytes {misses[seen][0]}..)"
                    )
                # A sparse block (SPARSE_OK) has no offset or size: GDAL reads it as nodata without a fetch.
                if off is not None and n is not None and int(n) > 0:
                    out.append((int(off), int(n)))
    return out


def _coalesce(
    ranges: List[Tuple[int, int]], gap: int = 16 * 1024
) -> Tuple[Tuple[int, int], ...]:
    """Sorted ranges, with neighbours closer than ``gap`` bytes joined (fewer fetches)."""
    out: List[List[int]] = []
    for off, n in sorted(ranges):
        if out and off <= out[-1][0] + out[-1][1] + gap:
            out[-1][1] = max(out[-1][1], off + n - out[-1][0])
        else:
            out.append([off, n])
    return tuple((o, n) for o, n in out)


def plan_ranges(
    head: bytes,
    size: int,
    geom=None,
    geom_crs: Optional[str] = None,
    resolution: Optional[float] = None,
) -> Plan:
    """The overview, window and byte ranges a clip to ``geom`` needs. ``geom`` None = the whole raster."""
    from databricks.labs.gbx.ds._window import window_for_geom

    opener = _Opener(size, [(0, head)])

    def too_large(why: object) -> HeaderTooLargeError:
        return HeaderTooLargeError(
            f"the COG header does not fit in the first {len(head)} bytes ({why}). "
            "Fix: write smaller COGs with cog_gbx (a smaller raster per file)"
        )

    try:
        with _open(opener) as ds:
            if not ds.block_shapes or ds.profile.get("tiled") is False:
                raise NotTiledError(
                    "the GeoTIFF is striped. Fix: write it as a COG with cog_gbx"
                )
            level = pick_overview(ds, resolution)
        with _open(opener, level) as lv:
            win: Optional[Tuple[int, int, int, int]] = (0, 0, lv.width, lv.height)
            if geom is not None:
                w = window_for_geom(lv, geom, geom_crs)
                win = (
                    None
                    if w is None
                    else (int(w.col_off), int(w.row_off), int(w.width), int(w.height))
                )
            ranges = (
                () if win is None else _coalesce(_block_ranges(lv, win, opener.misses))
            )
            plan = Plan(level, win, ranges)
    except (NotTiledError, HeaderTooLargeError):
        raise
    except Exception as exc:
        # GDAL fails a short read with a rasterio error; the miss is on the opener.
        if not opener.misses:
            raise
        raise too_large(exc) from exc
    if opener.misses:
        # GDAL can swallow a failed header read and answer with an empty value.
        gap = f"bytes {opener.misses[0][0]}.. were not fetched"
        raise too_large(gap) from MissingRangeError(gap)
    return plan
