"""The byte-level rules the functions over an ARRAY of tiles share (``rst_combine*``, ``rst_merge``, ``rst_frombands``):
which members of the array are used, how the result's cell id is chosen, which error tile an array of nothing else
answers, and the note a result carries for the members it skipped. Free of pyspark, so the session functions
(``pyrx.functions``) and the Unity Catalog handlers (``uc.tier_b``) apply one rule.
"""

from __future__ import annotations

from typing import Callable, Iterable, List, Optional, Sequence, Tuple

from rasterio.io import MemoryFile

from databricks.labs.gbx._gdal_policy import open_bytes


def collect_members(
    items: Iterable, to_bytes: Callable
) -> Tuple[List[bytes], list, int]:
    """``(rasters, kept, dropped)``: the bytes of each item ``to_bytes`` makes, in order, for the items whose bytes open
    as a raster. An item whose bytes cannot be made or opened is dropped (and counted), not an error: the others are
    combined. The probe opens the header only; a member is decoded where it is used.
    """
    rasters, kept, dropped = [], [], 0
    for item in items:
        try:
            candidate = bytes(to_bytes(item))
            with MemoryFile(candidate) as mf, open_bytes(mf):
                pass
        except (
            Exception
        ):  # noqa: BLE001 - an unreadable member is skipped, as the session does
            dropped += 1
            continue
        rasters.append(candidate)
        kept.append(item)
    return rasters, kept, dropped


def shared_cellid(cellids: Sequence[int]) -> int:
    """The cell id every member shares, else -1 (the rule of the heavyweight combine); 0 for no member."""
    ids = set(cellids)
    return next(iter(ids)) if len(ids) == 1 else (-1 if ids else 0)


def first_error(errors: Iterable[dict]) -> Optional[dict]:
    """The first of a group's error tiles (dicts), as it is. "First" is canonical, not arrival order: the least by cell
    id, then by the metadata sorted (the heavy tier orders them the same way). None for no errors.
    """

    def order(e):
        md = e.get("metadata") or {}
        cellid = e.get("cellid")
        return (
            -1 if cellid is None else int(cellid),
            "\0".join(f"{k}={v}" for k, v in sorted(md.items())),
        )

    return min(errors, key=order, default=None)


def skipped_note(op: str, dropped: int) -> str:
    """The ``last_error`` a result carries when ``dropped`` members could not be read and were skipped."""
    return f"{op}: skipped {dropped} corrupt input tile(s)"
