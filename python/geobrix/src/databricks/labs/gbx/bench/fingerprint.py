"""Cheap, deterministic output fingerprints for heavy-vs-light consistency checks.

A function's output is summarized so two API implementations can be compared for
numeric agreement (not byte-equality). Computed OUTSIDE the timed loop.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import numpy as np

from databricks.labs.gbx.pyrx import _serde

# 64-bit signed range bounds; H3 cell ids >= 2^63 (raw unsigned from the `h3`
# lib) are folded into the signed-int64 space the heavy LongMap uses, so the two
# engines hash to the same canonical id.
_TWO_63 = 2**63
_TWO_64 = 2**64


def _py(x):
    """Coerce numpy scalars to native Python types so json.dumps can serialize them."""
    if isinstance(x, np.floating):
        return float(x)
    if isinstance(x, np.integer):
        return int(x)
    if isinstance(x, np.bool_):
        return bool(x)
    return x


def _stat(arr_valid: np.ndarray) -> dict:
    if arr_valid.size == 0:
        return {"min": None, "max": None, "mean": None, "std": None}
    return {
        "min": float(np.min(arr_valid)),
        "max": float(np.max(arr_valid)),
        "mean": float(np.mean(arr_valid)),
        "std": float(np.std(arr_valid)),
    }


def _pool_valid_pixels(tile_bytes) -> np.ndarray:
    """Read every band of a raster tile, returning its valid (non-nodata) pixels.

    Used by the collection fingerprint to pool pixels across many output tiles.
    """
    parts = []
    with _serde.open_tile(bytes(tile_bytes)) as ds:
        nod = ds.nodata
        for bi in range(1, ds.count + 1):
            a = ds.read(bi)
            valid = a[a != nod] if nod is not None else a.ravel()
            parts.append(np.asarray(valid, dtype="float64").ravel())
    return np.concatenate(parts) if parts else np.empty(0, dtype="float64")


def fingerprint_collection(tiles) -> str:
    """Fingerprint a COLLECTION of output tiles (bucket C, group C4 tiling fns).

    The output is a LIST of tile byte strings. The fingerprint records the tile
    COUNT plus the agg stats pooled over ALL tiles' valid pixels across every
    band. Pooling is ORDER-INDEPENDENT, so heavy and light may emit tiles in any
    order and still agree, while the count is compared exactly.
    """
    tiles = list(tiles)
    pools = [_pool_valid_pixels(t) for t in tiles]
    pooled = np.concatenate(pools) if pools else np.empty(0, dtype="float64")
    return json.dumps(
        {"kind": "raster_collection", "count": len(tiles), "agg": _stat(pooled)},
        sort_keys=True,
    )


def fingerprint_output(out: Any) -> str:
    """Return a JSON string summarizing a function output for consistency comparison."""
    # Raster output: GTiff bytes -> per-band stats over valid (non-nodata) pixels.
    if isinstance(out, (bytes, bytearray)):
        bands = []
        with _serde.open_tile(bytes(out)) as ds:
            nod = ds.nodata
            for bi in range(1, ds.count + 1):
                a = ds.read(bi)
                valid = a[a != nod] if nod is not None else a.ravel()
                stat = _stat(np.asarray(valid, dtype="float64"))
                bands.append(
                    {
                        "shape": [int(a.shape[0]), int(a.shape[1])],
                        "dtype": str(a.dtype),
                        "nodata_count": int(a.size - valid.size),
                        **stat,
                    }
                )
        return json.dumps({"kind": "raster", "bands": bands}, sort_keys=True)
    if isinstance(out, (list, tuple)):
        # A LIST of raster tile bytes -> a raster_collection fingerprint (the C4
        # tiling fns return one). A list of scalars -> scalar_list (per-band
        # avg/min/max). Distinguish by element type: bytes => tile collection.
        if out and all(isinstance(x, (bytes, bytearray)) for x in out):
            return fingerprint_collection(out)
        return json.dumps(
            {"kind": "scalar_list", "values": [_py(x) for x in out]}, sort_keys=True
        )
    # Plain scalar.
    return json.dumps({"kind": "scalar", "value": _py(out)}, sort_keys=True)


def _signed_int64(cid) -> int:
    """Fold an id into the signed-int64 space the heavy LongMap stores.

    The `h3` lib's `str_to_int` yields an UNSIGNED 64-bit cell id (>= 2^63 for
    high-bit cells), while heavy stores cells as signed Longs. Canonicalizing
    both sides here lets the cell-id hash agree across engines (H3/quadbin ids
    are PARITY-comparable — verified by the parity gate in test_fingerprint.py).
    """
    cid = int(cid)
    return cid - _TWO_64 if cid >= _TWO_63 else cid


def _grid_records(cells):
    """Flatten the per-band grid output into (signed_cell_id, value) pairs.

    `gridagg.raster_to_grid` returns one list per band of
    ``{"cellID": int, "measure": float|int}``; `tessellate_h3` yields
    ``(cellid, bytes)`` tuples (no measure). Both shapes are accepted.
    """
    ids = []
    vals = []
    for band in cells:
        for rec in band:
            if isinstance(rec, dict):
                cid = rec.get("cellID")
                v = rec.get("measure")
            else:  # (cellid, _) tuple (tessellate)
                cid = rec[0]
                v = rec[1] if len(rec) > 1 else None
            ids.append(_signed_int64(cid))
            if v is not None and not isinstance(v, (bytes, bytearray)):
                vals.append(float(v))
    return ids, vals


def fingerprint_dggs_grid(cells) -> str:
    """Fingerprint a discrete-global-grid output (bucket B grid fns).

    The output is a set of cells (cell id + optional measure). The fingerprint
    records the cell COUNT, a sha256 of the SORTED signed-int64 cell ids
    (order-independent, and PARITY-comparable across engines), plus
    order-independent agg stats over the measures (``{}`` when there are none,
    e.g. tessellation). The comparator compares count exactly + agg in tolerance
    and treats an identical ``cells_hash`` as exact.
    """
    ids, vals = _grid_records(cells)
    sorted_ids = sorted(ids)
    joined = "\n".join(str(i) for i in sorted_ids)
    return json.dumps(
        {
            "kind": "dggs_grid",
            "count": len(sorted_ids),
            "cells_hash": hashlib.sha256(joined.encode()).hexdigest(),
            "cell_ids": sorted_ids,
            "agg": _stat(np.asarray(vals, dtype="float64")) if vals else {},
        },
        sort_keys=True,
    )


def _grid_records_str(cells):
    """Flatten a per-band BNG grid output into (string_cell_id, value) pairs.

    BNG ``gridagg.raster_to_grid`` returns one list per band of
    ``{"cellID": str, "measure": float|int}`` (OS grid reference STRINGS, e.g.
    ``"TQ3080"``), and BNG tessellation yields ``(cellid_str, bytes)`` tuples
    (no measure). Unlike H3/quadbin the cell id is a STRING, so it is kept as-is
    (no signed-int64 fold). Both shapes are accepted.
    """
    ids = []
    vals = []
    for band in cells:
        for rec in band:
            if isinstance(rec, dict):
                cid = rec.get("cellID")
                v = rec.get("measure")
            else:  # (cellid, _) tuple (tessellate)
                cid = rec[0]
                v = rec[1] if len(rec) > 1 else None
            ids.append(str(cid))
            if v is not None and not isinstance(v, (bytes, bytearray)):
                vals.append(float(v))
    return ids, vals


def fingerprint_dggs_grid_str(cells) -> str:
    """Fingerprint a STRING-cell-id discrete-global-grid output (BNG grid fns).

    The BNG analogue of :func:`fingerprint_dggs_grid`: BNG cell ids are OS grid
    reference STRINGS (e.g. ``"TQ3080"``), not Longs, so the cell-set hash is
    over the SORTED string ids (no signed-int64 fold). Records the cell COUNT, a
    sha256 of the sorted string ids, the sorted ids, plus order-independent agg
    stats over the measures (``{}`` when there are none, e.g. tessellation). The
    light pygx ``_bng`` tier emits the SAME string ids as the heavy tier, so an
    identical ``cells_hash`` is an exact cell-set match (mirrors the Scala
    ``BenchFingerprint.ofDggsGridStr`` / ``ofDggsGridStrIds``).
    """
    ids, vals = _grid_records_str(cells)
    sorted_ids = sorted(ids)
    joined = "\n".join(sorted_ids)
    return json.dumps(
        {
            "kind": "dggs_grid",
            "count": len(sorted_ids),
            "cells_hash": hashlib.sha256(joined.encode()).hexdigest(),
            "cell_ids": sorted_ids,
            "agg": _stat(np.asarray(vals, dtype="float64")) if vals else {},
        },
        sort_keys=True,
    )


def _vector_features(features):
    """Split features into (geometries, attributes).

    `analysis.contour` returns ``[{"geom_wkb": bytes, "value": float}]`` and
    `features.polygonize` returns ``[(geom_wkb, value)]``; both are accepted.
    """
    import shapely.wkb

    geoms = []
    attrs = []
    for feat in features:
        if isinstance(feat, dict):
            wkb = feat.get("geom_wkb")
            a = feat.get("value")
        else:  # (geom_wkb, value) tuple
            wkb, a = feat[0], feat[1]
        geoms.append(shapely.wkb.loads(bytes(wkb)))
        if a is not None:
            attrs.append(float(a))
    return geoms, attrs


def fingerprint_vector(features) -> str:
    """Fingerprint a vector-feature output (bucket B vector fns).

    The output is a set of geometry features (contour LineStrings, polygonize
    Polygons). The fingerprint records the feature COUNT, the total ``measure``
    (line length for lines, polygon area otherwise — chosen by geometry type),
    and order-independent agg stats over the feature attributes. Summing the
    per-feature measure and pooling the attributes are ORDER-INDEPENDENT, so the
    two engines may emit features in any order and still agree; count is
    compared exactly.
    """
    geoms, attrs = _vector_features(features)
    is_lines = any(g.geom_type in ("LineString", "MultiLineString") for g in geoms)
    if is_lines:
        measure = float(sum(g.length for g in geoms))
    else:
        measure = float(sum(g.area for g in geoms))
    return json.dumps(
        {
            "kind": "vector",
            "count": len(geoms),
            "measure": measure,
            "attr_agg": _stat(np.asarray(attrs, dtype="float64")) if attrs else {},
        },
        sort_keys=True,
    )
