"""Decode-free COG detection + format-metadata stamping.

One header-sniff core (`sniff_header`) is the single source of format truth,
shared by `detect_cog` (read/route) and `stamp_format_metadata` (write/heal) so
the two can never disagree. Sniffing parses only the TIFF header + IFDs (a few
hundred bytes); it never decodes pixels.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Dict, Optional

GBX_FORMAT = "gbx_format"
GBX_BLOCKSIZE = "gbx_blocksize"
GBX_OVERVIEW_LEVELS = "gbx_overview_levels"

_TAG_TILE_WIDTH = 322
_TAG_SUBFILE_TYPE = 254  # bit 0 = reduced-resolution (overview)
_TYPE_SIZES = {1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 6: 1, 7: 1, 8: 2, 9: 4, 10: 8, 11: 4, 12: 8}


@dataclass(frozen=True)
class CogInfo:
    is_cog: bool
    tiled: bool
    blocksize: Optional[int]
    overview_levels: int


_NON_COG = CogInfo(is_cog=False, tiled=False, blocksize=None, overview_levels=0)


def _read_ifd(buf, off, endian):
    """Return (tags: dict[tag]->(type,count,value_or_offset), next_ifd_off)."""
    (n,) = struct.unpack_from(endian + "H", buf, off)
    tags = {}
    p = off + 2
    for _ in range(n):
        tag, typ, count = struct.unpack_from(endian + "HHI", buf, p)
        (value,) = struct.unpack_from(endian + "I", buf, p + 8)
        tags[tag] = (typ, count, value)
        p += 12
    (next_off,) = struct.unpack_from(endian + "I", buf, p)
    return tags, next_off


def sniff_header(raster_bytes: bytes) -> CogInfo:
    """Classify raster bytes by TIFF header/IFD structure only (no pixel decode).

    A COG here = internally tiled AND has >=1 reduced-resolution overview IFD.
    Any parse failure (non-TIFF, truncated, BigTIFF we don't walk) -> non-COG.
    """
    try:
        buf = bytes(raster_bytes)
        if len(buf) < 8:
            return _NON_COG
        bo = buf[:2]
        if bo == b"II":
            endian = "<"
        elif bo == b"MM":
            endian = ">"
        else:
            return _NON_COG
        (magic,) = struct.unpack_from(endian + "H", buf, 2)
        if magic != 42:  # 43 = BigTIFF; not walked here -> treated as non-COG
            return _NON_COG
        (ifd_off,) = struct.unpack_from(endian + "I", buf, 4)

        tiled = False
        blocksize = None
        overview_levels = 0
        ifd_index = 0
        while ifd_off != 0 and ifd_index < 64:
            tags, ifd_off = _read_ifd(buf, ifd_off, endian)
            if ifd_index == 0:
                if _TAG_TILE_WIDTH in tags:
                    tiled = True
                    blocksize = int(tags[_TAG_TILE_WIDTH][2])
            else:
                subfile = tags.get(_TAG_SUBFILE_TYPE)
                if subfile and (int(subfile[2]) & 0x1):
                    overview_levels += 1
            ifd_index += 1

        is_cog = tiled and overview_levels >= 1
        return CogInfo(is_cog=is_cog, tiled=tiled, blocksize=blocksize,
                       overview_levels=overview_levels)
    except Exception:
        return _NON_COG


def detect_cog(metadata: Optional[Dict[str, str]], raster_bytes: bytes) -> CogInfo:
    """R1 resolver: trust the metadata flag when present, else sniff the bytes."""
    if metadata:
        flag = metadata.get(GBX_FORMAT)
        if flag is not None:
            is_cog = str(flag).lower() == "cog"
            try:
                ovr = int(metadata.get(GBX_OVERVIEW_LEVELS, "0"))
            except (TypeError, ValueError):
                ovr = 0
            try:
                blk = int(metadata[GBX_BLOCKSIZE]) if metadata.get(GBX_BLOCKSIZE) else None
            except (TypeError, ValueError):
                blk = None
            return CogInfo(is_cog=is_cog, tiled=is_cog or blk is not None,
                          blocksize=blk, overview_levels=ovr)
    return sniff_header(raster_bytes)


def stamp_format_metadata(
    raster_bytes: bytes, existing_metadata: Optional[Dict[str, str]]
) -> Dict[str, str]:
    """R2 writer/healer: re-derive gbx_* from ACTUAL bytes, merged over existing."""
    info = sniff_header(raster_bytes)
    md = dict(existing_metadata or {})
    md[GBX_FORMAT] = "cog" if info.is_cog else "gtiff"
    md[GBX_OVERVIEW_LEVELS] = str(info.overview_levels)
    if info.blocksize is not None:
        md[GBX_BLOCKSIZE] = str(info.blocksize)
    return md
