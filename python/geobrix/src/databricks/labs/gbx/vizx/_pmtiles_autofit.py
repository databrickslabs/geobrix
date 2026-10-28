"""Auto-fit reduction of an oversized PMTiles archive for interactive embedding.

GeoBrix vizx targets Databricks notebooks, where an interactive map is a single
base64-embedded HTML cell capped by the Serverless cell-output limit (10 MB
default, 20 MB max). A large archive cannot embed; ``interactive_fit='downzoom'`` keeps
the experience interactive by *investing little*: drop the highest (densest)
zoom levels until the base64-rendered archive fits the budget. The user gets one
interactive map of the whole extent at reduced detail.

This is tier-agnostic (raster OR vector tiles): it rebuilds a smaller archive
from the tiles already present via the same in-memory ``Writer`` /
``build_header_info`` assembler used by ``gbx_pmtiles_agg`` -- no re-tiling, no
tippecanoe, no tile-join.

The full-detail, lossless counterpart -- ``interactive_fit='all'`` (spatially shard
into per-region sub-archives, each under budget, rendered as a multi-shard
interactive experience) -- is the planned "halo" feature and is not built here.
"""

from __future__ import annotations

from typing import Tuple

# Base64 inflation factor (3 raw bytes -> 4 ASCII chars). Mirrors _maplibre.
_BASE64_INFLATION = 4.0 / 3.0


def _rendered_bytes(archive_len: int) -> float:
    """Approximate the base64-rendered embed size of an archive of this length."""
    return archive_len * _BASE64_INFLATION


def _rebuild_with_max_zoom(raw: bytes, keep_max_z: int) -> bytes:
    """Rebuild a PMTiles archive keeping only tiles at zoom <= ``keep_max_z``.

    Preserves tile type and tile content; the header bounds/zoom range are
    recomputed from the kept tiles via the shared header builder.
    """
    from pmtiles.reader import MemorySource, Reader
    from pmtiles.tile import Compression, zxy_to_tileid
    from pmtiles.writer import Writer

    from databricks.labs.gbx.ds._pmtiles_read import iter_tiles
    from databricks.labs.gbx.ds.tiles._header import build_header_info, sniff_tile_type
    from databricks.labs.gbx.ds.tiles.grid import SlippyGrid
    from databricks.labs.gbx.pmtiles import pmtiles_info

    info = pmtiles_info(raw)
    metadata = info.get("metadata") or {}
    # The tile payloads are copied through verbatim, so the rebuilt header MUST
    # declare the SOURCE's tile_compression -- not a hardcoded NONE. tippecanoe (and
    # gbx_pmtiles_agg) gzip their MVT tiles; mislabeling them NONE makes pmtiles.js
    # skip the gunzip and feed gzipped bytes to the MVT decoder -> zero features
    # render (a silently blank map).
    src_compression = (
        Reader(MemorySource(raw)).header().get("tile_compression", Compression.NONE)
    )

    kept = []  # (z, x, y, payload)
    first_payload = None
    for (z, x, y), payload in iter_tiles(raw):
        if z > keep_max_z:
            continue
        if first_payload is None:
            first_payload = payload
        kept.append((z, x, y, payload))

    if not kept:
        # Nothing at/below the cutoff (shouldn't happen for keep_max_z >= min_zoom);
        # return the original so the caller can decide.
        return raw

    tile_type = sniff_tile_type(first_payload)
    coords = [(z, x, y) for (z, x, y, _) in kept]
    hdr = build_header_info(coords, SlippyGrid(), tile_type, src_compression, metadata)

    # Preserve the SOURCE archive's data bounds. Dropping the highest zoom levels does not
    # change the data extent, but build_header_info recomputes bounds as the union of the
    # KEPT tiles' grid bboxes — for a low-zoom overview (z0-9) that balloons to a huge
    # coarse-tile extent (e.g. a z2 quarter-of-world), which then makes the viewer's
    # fitBounds open on the globe. Keep the original bounds so the downzoomed archive still
    # frames its data.
    src_bounds = info.get("bounds")
    if src_bounds and len(src_bounds) >= 4:
        hdr.bbox = (
            float(src_bounds[0]),
            float(src_bounds[1]),
            float(src_bounds[2]),
            float(src_bounds[3]),
        )

    import io

    buf = io.BytesIO()
    writer = Writer(buf)
    for z, x, y, payload in sorted(kept, key=lambda t: zxy_to_tileid(t[0], t[1], t[2])):
        writer.write_tile(zxy_to_tileid(z, x, y), payload)
    writer.finalize(hdr.header_dict(), hdr.metadata)
    return buf.getvalue()


def autofit_archive(raw: bytes, *, max_embed_mb: float) -> Tuple[bytes, dict]:
    """Reduce ``raw`` (a PMTiles archive) to fit the interactive embed budget.

    Drops the highest zoom levels one at a time until the base64-rendered archive
    size is within ``max_embed_mb``. Always keeps at least the coarsest zoom level
    (a degenerate last resort) so the caller never gets an empty archive; if even
    that exceeds the budget, ``report["fits"]`` is ``False`` so the caller can
    route to the static fallback instead.

    Args:
        raw: The source PMTiles archive bytes.
        max_embed_mb: Target embed budget in mebibytes (must be > 0). Compared
            against the base64-rendered size (~4/3x the archive bytes).

    Returns:
        ``(reduced_bytes, report)`` where ``report`` is::

            {
                "fits": bool,            # rendered size <= budget
                "original_max_zoom": int,
                "kept_max_zoom": int,
                "dropped_zooms": [int],  # zoom levels removed (descending)
                "original_bytes": int,
                "reduced_bytes": int,
            }

        ``reduced_bytes is raw`` (unchanged object) when the archive already fits.
    """
    if max_embed_mb <= 0:
        raise ValueError(
            f"autofit_archive: max_embed_mb must be > 0; got {max_embed_mb}"
        )

    from databricks.labs.gbx.pmtiles import pmtiles_info

    budget_bytes = max_embed_mb * 1_048_576

    info = pmtiles_info(raw)
    original_max_z = int(info["max_zoom"])
    min_z = int(info["min_zoom"])

    # Already fits -> no-op (return the same object so callers can identity-check).
    if _rendered_bytes(len(raw)) <= budget_bytes:
        return raw, {
            "fits": True,
            "original_max_zoom": original_max_z,
            "kept_max_zoom": original_max_z,
            "dropped_zooms": [],
            "original_bytes": len(raw),
            "reduced_bytes": len(raw),
        }

    dropped: list[int] = []
    reduced = raw
    kept_max_z = original_max_z

    # Step the max zoom down until we fit or we hit the coarsest level.
    for keep_max_z in range(original_max_z - 1, min_z - 1, -1):
        dropped.append(keep_max_z + 1)
        reduced = _rebuild_with_max_zoom(raw, keep_max_z)
        kept_max_z = keep_max_z
        if _rendered_bytes(len(reduced)) <= budget_bytes:
            break

    fits = _rendered_bytes(len(reduced)) <= budget_bytes
    return reduced, {
        "fits": fits,
        "original_max_zoom": original_max_z,
        "kept_max_zoom": kept_max_z,
        "dropped_zooms": dropped,
        "original_bytes": len(raw),
        "reduced_bytes": len(reduced),
    }
