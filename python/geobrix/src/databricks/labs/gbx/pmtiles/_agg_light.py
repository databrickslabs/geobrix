"""Lightweight gbx_pmtiles_agg — tier-neutral grouped aggregate.

PMTiles archives raster OR vector tiles, so this lives in the pmtiles package
(not pyrx/pyvx) and is registered from BOTH light tiers. Reuses the ds.tiles
assembler; writes to an in-memory BytesIO. Serverless-safe: spark.udf.register +
Column expressions only (no _jvm / spark.conf / rdd).
"""

from __future__ import annotations

import io
import json
from typing import Optional, Sequence

import mapbox_vector_tile as mvt
import pandas as pd
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer
from pyspark.sql import Column, SparkSession  # noqa: F401
from pyspark.sql import functions as f  # noqa: F401
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BinaryType

from databricks.labs.gbx.ds.tiles._header import build_header_info, sniff_tile_type
from databricks.labs.gbx.ds.tiles.grid import SlippyGrid

# Mirror heavy PMTilesAcc's 100 MiB accumulation cap so the failure mode matches.
_MAX_ARCHIVE_BYTES = 100 * 1024 * 1024

# Set True by register_pmtiles_agg; only the fallback wrapper path consults it.
_LIGHT_REGISTERED = False


def _merge_mvt_blobs(blobs: list[bytes], extent: int = 4096) -> bytes:
    """Decode multiple single-feature MVT blobs and union features per layer name.

    Geometry stays in tile-local [0, extent] integer space — no reprojection
    (each blob is already tile-local for the same (z,x,y); decode/encode round-trips
    the local coords). Attributes are preserved per feature.

    Decode and encode MUST use the SAME y orientation. Tiles are written y-down
    (``y_coord_down=True``, MVT spec: origin top-left, y increases south). ``mvt.decode``
    defaults to y-UP, so decoding without ``y_coord_down=True`` and re-encoding with it
    FLIPS every merged tile's y (latitude mirrored within the tile) — features jump as
    the tile grid changes per zoom. Decode y-down so the round-trip is identity.

    Returns one merged MVT blob. If the list has a single blob, returns it directly
    to avoid a decode/encode round-trip for the common single-feature case.
    """
    if len(blobs) == 1:
        return blobs[0]
    layers: dict[str, list] = {}
    for blob in blobs:
        try:
            decoded = mvt.decode(blob, default_options={"y_coord_down": True})
        except Exception:
            # Malformed blob: skip rather than crashing the whole group.
            continue
        for layer_name, layer_data in decoded.items():
            layers.setdefault(layer_name, []).extend(layer_data.get("features", []))
    if not layers:
        return blobs[0]  # nothing decoded cleanly; fall back to first
    tile_spec = [{"name": name, "features": feats} for name, feats in layers.items()]
    return mvt.encode(
        tile_spec, default_options={"extents": extent, "y_coord_down": True}
    )


def _assemble_archive(
    data: Sequence,
    zs: Sequence,
    xs: Sequence,
    ys: Sequence,
    metadata: Optional[dict] = None,
) -> Optional[bytes]:
    """Fold a group's (bytes, z, x, y) tiles into one PMTiles v3 archive (bytes).

    Null payloads are skipped; an all-null/empty group returns None. For vector
    (MVT) tiles, multiple blobs for the same (z,x,y) are merged into one
    multi-feature tile (decode each, union features per layer, re-encode).
    For raster (PNG/JPEG/WebP), first-write-wins is preserved. Tiles are
    written in ascending Hilbert TileID order.
    """
    # Phase 1: accumulate all non-null payloads per tileid.
    tileid_payloads: dict[int, list[bytes]] = {}
    tileid_coords: dict[int, tuple[int, int, int]] = {}
    total = 0
    first_payload = None
    for d, z, x, y in zip(data, zs, xs, ys):
        if d is None:
            continue
        b = bytes(d)
        total += len(b)
        if total > _MAX_ARCHIVE_BYTES:
            raise ValueError(
                f"pmtiles_agg group payload exceeds {_MAX_ARCHIVE_BYTES} bytes; "
                "split into more groups or fewer tiles per archive"
            )
        tileid = zxy_to_tileid(int(z), int(x), int(y))
        if first_payload is None:
            first_payload = b
        tileid_payloads.setdefault(tileid, []).append(b)
        tileid_coords[tileid] = (int(z), int(x), int(y))

    if not tileid_payloads:
        return None

    tile_type = sniff_tile_type(first_payload)
    is_vector = tile_type == TileType.MVT

    # Phase 2: resolve each tileid to one output blob.
    tiles = []
    for tileid in sorted(tileid_payloads.keys()):
        z, x, y = tileid_coords[tileid]
        payloads = tileid_payloads[tileid]
        if is_vector and len(payloads) > 1:
            resolved = _merge_mvt_blobs(payloads)
        else:
            resolved = payloads[0]
        tiles.append((z, x, y, tileid, resolved))

    info = build_header_info(
        [(z, x, y) for (z, x, y, _, _) in tiles],
        SlippyGrid(),
        tile_type,
        Compression.NONE,
        metadata or {},
    )
    buf = io.BytesIO()
    writer = Writer(buf)
    for _, _, _, tileid, b in tiles:  # already sorted by tileid
        writer.write_tile(tileid, b)
    writer.finalize(info.header_dict(), info.metadata)
    return buf.getvalue()


@pandas_udf(BinaryType())
def _pmtiles_agg_udf(
    data: pd.Series,
    z: pd.Series,
    x: pd.Series,
    y: pd.Series,
    metadata_json: pd.Series = None,
) -> Optional[bytes]:
    """GROUPED_AGG: fold one group's tiles into a PMTiles archive (BINARY).

    ``metadata_json`` is optional so the 4-arg SQL form
    ``gbx_pmtiles_agg(tile, z, x, y)`` resolves; the DataFrame wrapper always
    supplies it. Blank / whitespace-only / ``"{}"`` payloads mean no metadata.
    """
    meta = {}
    if metadata_json is not None and len(metadata_json) > 0:
        for m in metadata_json:
            if m is not None and str(m).strip():
                meta = json.loads(m)
                break
    return _assemble_archive(data, z, x, y, meta)


def register_pmtiles_agg(spark: SparkSession = None) -> None:
    """Register the light gbx_pmtiles_agg grouped aggregate (Serverless-safe).

    Called by both pyrx.register and pyvx.register, and usable standalone.
    """
    global _LIGHT_REGISTERED
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    spark.udf.register("gbx_pmtiles_agg", _pmtiles_agg_udf)
    _LIGHT_REGISTERED = True
