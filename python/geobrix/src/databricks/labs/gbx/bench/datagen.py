"""Seeded, valid-at-scale raster tile generator for benchmarking."""

from __future__ import annotations

import itertools
from pathlib import Path

import numpy as np
import rasterio
import shapely.geometry
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.bench import manifest as m

# CRS -> (origin_x, origin_y, pixel_size in CRS units) for a consistent affine.
_CRS_GEO = {
    4326: (-73.99, 40.75, 0.0001),  # WGS84 degrees (NYC-ish)
    3857: (-8237000.0, 4970000.0, 10.0),  # WebMercator metres
    32618: (583000.0, 4507000.0, 10.0),  # UTM 18N metres
    27700: (530000.0, 180000.0, 10.0),  # BNG metres (London)
}

_NODATA = {"uint8": 255, "int16": -9999, "float32": -9999.0}
# Values represent non-negative reflectance/elevation-like magnitudes; keeping
# them >= 0 guarantees spectral-index validity across all dtypes (e.g. NDVI =
# (nir-red)/(nir+red) stays in [-1, 1] because the denominator never crosses
# zero). Terrain ops are unaffected -- they use gradients.
_DTYPE_RANGE = {"uint8": (0, 254), "int16": (0, 1000), "float32": (0.0, 1.0)}


def _base_field(tile_px: int, rng: np.random.Generator) -> np.ndarray:
    """A smooth gradient + low-amplitude noise + sinusoid, in [0,1]."""
    y, x = np.mgrid[0:tile_px, 0:tile_px].astype("float64") / max(tile_px - 1, 1)
    grad = 0.5 * (x + y) / 2.0 + 0.5 * x  # ramp
    sin = 0.15 * np.sin(6.0 * np.pi * x) * np.cos(6.0 * np.pi * y)
    noise = 0.05 * rng.standard_normal((tile_px, tile_px))
    f = grad + sin + noise
    f -= f.min()
    f /= max(f.max(), 1e-9)
    return f  # [0,1]


def _to_dtype(f01: np.ndarray, dtype: str) -> np.ndarray:
    lo, hi = _DTYPE_RANGE[dtype]
    arr = lo + f01 * (hi - lo)
    return arr.astype(dtype)


def make_tile_bytes(
    tile_px: int,
    bands: int,
    dtype: str,
    srid: int,
    nodata_frac: float,
    seed: int,
    nodata_mode: str = "sparse",
) -> bytes:
    """Generate one valid GeoTIFF tile as in-memory bytes (deterministic per seed).

    With ``nodata_mode="sparse"`` (default), the requested ``nodata_frac`` is hit
    exactly via an exact-count random pixel mask. With ``nodata_mode="border"``,
    the nodata region is an approximate frame whose actual fraction can diverge
    from ``nodata_frac`` (especially for small tiles or extreme fractions).
    """
    rng = np.random.default_rng(seed)
    ox, oy, px = _CRS_GEO[srid]
    transform = from_origin(ox, oy, px, px)
    nodata = _NODATA[dtype]

    base = _base_field(tile_px, rng)  # [0,1]
    data = np.empty((bands, tile_px, tile_px), dtype=dtype)
    for bi in range(bands):
        # Band-correlated: each band a monotone transform of the shared field,
        # so spectral indices (NDVI etc.) are non-degenerate and in-range.
        shifted = np.clip(base ** (1.0 + 0.3 * bi) + 0.02 * bi, 0.0, 1.0)
        data[bi] = _to_dtype(shifted, dtype)

    if nodata_frac > 0:
        n = int(round(nodata_frac * tile_px * tile_px))
        if nodata_mode == "border":
            mask = np.zeros((tile_px, tile_px), dtype=bool)
            w = max(1, int(round(nodata_frac * tile_px / 4)))
            mask[:w, :] = mask[-w:, :] = mask[:, :w] = mask[:, -w:] = True
        else:  # "sparse" (default): exact-count random pixel mask
            flat = rng.choice(
                tile_px * tile_px, size=min(n, tile_px * tile_px), replace=False
            )
            mask = np.zeros(tile_px * tile_px, dtype=bool)
            mask[flat] = True
            mask = mask.reshape(tile_px, tile_px)
        for bi in range(bands):
            data[bi][mask] = nodata

    profile = {
        "driver": "GTiff",
        "width": tile_px,
        "height": tile_px,
        "count": bands,
        "dtype": dtype,
        "crs": rasterio.crs.CRS.from_epsg(srid),
        "transform": transform,
        "nodata": nodata,
    }
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data)
        return bytes(mf.read())


def _bounds_and_z_sampler(tile_meta_or_bounds):
    """Resolve (left, bottom, right, top) bounds + a z-sampler from the input.

    Accepts either an open rasterio ``DatasetReader`` (preferred: z-points sample
    the actual tile band so Z is realistic) or a plain ``(left, bottom, right,
    top)`` bounds tuple (z-points fall back to a seeded surface). Returns the
    bounds tuple and a ``sample(xs, ys, rng) -> z_array`` callable.
    """
    if hasattr(tile_meta_or_bounds, "bounds") and hasattr(tile_meta_or_bounds, "read"):
        ds = tile_meta_or_bounds
        left, bottom, right, top = tuple(ds.bounds)
        band1 = ds.read(1).astype("float64")
        transform = ds.transform

        def sample(xs, ys, rng):
            # world -> pixel (row, col) via the inverse affine, clamped in-range.
            cols, rows = ~transform * (np.asarray(xs), np.asarray(ys))
            rows = np.clip(rows.astype(int), 0, band1.shape[0] - 1)
            cols = np.clip(cols.astype(int), 0, band1.shape[1] - 1)
            return band1[rows, cols]

        return (left, bottom, right, top), sample

    left, bottom, right, top = tuple(tile_meta_or_bounds)

    def sample(xs, ys, rng):
        # No tile to read: a deterministic seeded surface over the bounds.
        nx = (np.asarray(xs) - left) / max(right - left, 1e-9)
        ny = (np.asarray(ys) - bottom) / max(top - bottom, 1e-9)
        return 0.5 * (nx + ny)

    return (left, bottom, right, top), sample


def generate_geometry_corpus(
    tile_meta_or_bounds,
    srid: int,
    seed: int,
    *,
    n_boxes: int = 16,
    n_points: int = 64,
) -> m.GeometrySet:
    """Derive a deterministic geometry set from a tile's bounds + CRS.

    Every box/point lies WITHIN the tile bounds (so it is in-extent on the tile's
    own CRS), each carries a deterministic burn value, and z-points sample the
    actual tile band (realistic elevation) when an open dataset is passed, else a
    seeded surface. WKB carries no CRS -- the recorded ``srid`` is the contract.

    ``tile_meta_or_bounds`` is an open rasterio ``DatasetReader`` (preferred) or a
    ``(left, bottom, right, top)`` bounds tuple. Returns a ``manifest.GeometrySet``.
    """
    (left, bottom, right, top), z_sample = _bounds_and_z_sampler(tile_meta_or_bounds)
    rng = np.random.default_rng(seed)
    w = right - left
    h = top - bottom

    # Boxes: axis-aligned, each shrunk to a random 5-15% of the extent and offset
    # so it stays fully inside the bounds. Burn value in [0, 1).
    boxes = []
    bw = w * (0.05 + 0.10 * rng.random(n_boxes))
    bh = h * (0.05 + 0.10 * rng.random(n_boxes))
    bx = left + (w - bw) * rng.random(n_boxes)
    by = bottom + (h - bh) * rng.random(n_boxes)
    bvals = rng.random(n_boxes)
    for i in range(n_boxes):
        geom = shapely.geometry.box(bx[i], by[i], bx[i] + bw[i], by[i] + bh[i])
        boxes.append((geom.wkb, float(bvals[i])))

    # Points: scattered strictly inside the bounds; burn value in [0, 1).
    px = left + w * rng.random(n_points)
    py = bottom + h * rng.random(n_points)
    pvals = rng.random(n_points)
    points = [
        (shapely.geometry.Point(float(px[i]), float(py[i])).wkb, float(pvals[i]))
        for i in range(n_points)
    ]

    # Z-points: same XY scatter, Z sampled from the source tile (or seeded surface).
    zx = left + w * rng.random(n_points)
    zy = bottom + h * rng.random(n_points)
    zz = z_sample(zx, zy, rng)
    zpoints = [
        shapely.geometry.Point(float(zx[i]), float(zy[i]), float(zz[i])).wkb
        for i in range(n_points)
    ]

    source_tile = getattr(tile_meta_or_bounds, "name", "") or ""
    return m.GeometrySet(
        srid=int(srid),
        source_tile=str(source_tile),
        boxes=boxes,
        points=points,
        zpoints=zpoints,
    )


def generate_corpus(
    out_dir,
    seed,
    tile_px,
    bands,
    dtypes,
    srids,
    nodata_fracs,
    row_rows,
    row_tile_px,
    row_bands,
    row_dtype,
) -> m.Corpus:
    out_dir = Path(out_dir)
    (out_dir / "size").mkdir(parents=True, exist_ok=True)
    (out_dir / "rows").mkdir(parents=True, exist_ok=True)

    size_sweep = []
    cellid = 0
    # one tile per (tile_px, bands, dtype) cycling srid + nodata_frac for variety
    combos = list(itertools.product(tile_px, bands, dtypes))
    for i, (tp, bd, dt) in enumerate(combos):
        srid = srids[i % len(srids)]
        ndf = nodata_fracs[i % len(nodata_fracs)]
        tile_seed = seed + cellid
        b = make_tile_bytes(tp, bd, dt, srid, ndf, tile_seed)
        rel = f"size/t{cellid}_{tp}px_{bd}b_{dt}_{srid}.tif"
        (out_dir / rel).write_bytes(b)
        size_sweep.append(m.TileEntry(rel, cellid, srid, dt, bd, tp, ndf))
        cellid += 1

    # Great-Britain-overlapping tile for the BNG raster->grid / tessellate fns.
    # Those functions reproject the tile to EPSG:27700 internally and drop any
    # pixel that falls outside GB (BNG.isValid). Every ordinary sweep tile is
    # NYC-ish, so warping it to 27700 lands outside Britain -> an EMPTY grid, and
    # the cross-tier comparison becomes a vacuous both-empty match. This tile is
    # 27700-native over central London (see _CRS_GEO[27700]), so BNG binning lands
    # REAL cells on BOTH tiers. It is tagged role="bng_gb": the runners feed it ONLY
    # to the BNG raster-input fns and skip it for every other function (whose tile
    # selection is therefore unchanged). It matches the first sweep tile's
    # band-count / dtype / pixel conventions so the synth + heavy-read path is
    # identical to a normal tile in every respect but its CRS + extent.
    gb_tp, gb_bd, gb_dt = combos[0]
    gb_ndf = nodata_fracs[0]
    gb_seed = seed + cellid
    gb_bytes = make_tile_bytes(gb_tp, gb_bd, gb_dt, 27700, gb_ndf, gb_seed)
    gb_rel = f"size/t{cellid}_{gb_tp}px_{gb_bd}b_{gb_dt}_27700_bng_gb.tif"
    (out_dir / gb_rel).write_bytes(gb_bytes)
    size_sweep.append(
        m.TileEntry(gb_rel, cellid, 27700, gb_dt, gb_bd, gb_tp, gb_ndf, role="bng_gb")
    )
    cellid += 1

    row_tiles = []
    for j in range(row_rows):
        srid = srids[j % len(srids)]
        tile_seed = seed + 100000 + j
        b = make_tile_bytes(row_tile_px, row_bands, row_dtype, srid, 0.0, tile_seed)
        rel = f"rows/r{j}.tif"
        (out_dir / rel).write_bytes(b)
        row_tiles.append(
            m.TileEntry(
                rel, len(size_sweep) + j, srid, row_dtype, row_bands, row_tile_px, 0.0
            )
        )

    corpus = m.Corpus(
        seed=seed,
        size_sweep=size_sweep,
        row_pool=m.RowPool(row_tile_px, row_bands, row_dtype, row_tiles),
    )
    corpus.write(out_dir / "corpus.json")
    _write_geometry_corpus(out_dir, corpus, seed)
    return corpus


# Geometry is derived from the FIRST size-sweep tile of each distinct CRS, so the
# geometry corpus stays cheap (one set per CRS) yet covers every projection the
# tile sweep uses. Each set's geometry is in-extent for its source tile; z-points
# sample that tile's band 1. The representative manifest provenance is the first
# size-sweep tile. Deterministic: the geometry seed is derived from the corpus
# seed + the tile cellid, so re-running gen-data reproduces byte-identical WKB.
def _write_geometry_corpus(out_dir, corpus: m.Corpus, seed: int) -> m.GeometryCorpus:
    out_dir = Path(out_dir)
    sets: dict = {}
    seen_srid = set()
    rep = None
    for te in corpus.size_sweep:
        if te.srid in seen_srid:
            continue
        seen_srid.add(te.srid)
        with rasterio.open(out_dir / te.path) as ds:
            gset = generate_geometry_corpus(ds, te.srid, seed + te.cellid)
        # Record the corpus-relative tile path (not the absolute ds.name) so the
        # heavy tier resolves it under the same corpus root.
        gset = m.GeometrySet(
            srid=gset.srid,
            source_tile=te.path,
            boxes=gset.boxes,
            points=gset.points,
            zpoints=gset.zpoints,
        )
        sets[f"srid_{te.srid}"] = gset
        if rep is None:
            rep = te
    gc = m.GeometryCorpus(
        seed=seed,
        srid=(rep.srid if rep else 0),
        source_tile=(rep.path if rep else ""),
        sets=sets,
    )
    gc.write(out_dir / "geometry.json")
    return gc


def validity_gate(root, corpus: m.Corpus, nodata_warn_threshold: float = 0.9):
    """Return a list of problem strings; empty means the corpus is valid."""
    root = Path(root)
    problems = []
    all_tiles = list(corpus.size_sweep) + list(corpus.row_pool.tiles)
    for te in all_tiles:
        p = root / te.path
        if not p.exists():
            problems.append(f"missing: {te.path}")
            continue
        try:
            with rasterio.open(p) as ds:
                if ds.width != te.tile_px or ds.height != te.tile_px:
                    problems.append(
                        f"{te.path}: size {ds.width}x{ds.height} != {te.tile_px}"
                    )
                if ds.count != te.bands:
                    problems.append(f"{te.path}: bands {ds.count} != {te.bands}")
                if ds.crs is None or ds.crs.to_epsg() != te.srid:
                    problems.append(f"{te.path}: crs {ds.crs} != {te.srid}")
                arr = ds.read(1)
                if ds.nodata is not None:
                    frac = float((arr == ds.nodata).mean())
                    if frac > nodata_warn_threshold:
                        problems.append(
                            f"{te.path}: nodata frac {frac:.2f} > {nodata_warn_threshold}"
                        )
        except Exception as e:  # noqa: BLE001
            problems.append(f"{te.path}: open failed: {e}")
    return problems


def _parse_int_list(s: str):
    return [int(x) for x in s.split(",") if x.strip()]


def _parse_float_list(s: str):
    return [float(x) for x in s.split(",") if x.strip()]


def main(argv=None):
    import argparse
    import json

    ap = argparse.ArgumentParser(prog="bench.datagen")
    ap.add_argument("--out", required=True)
    ap.add_argument("--seed", type=int, default=1234)
    ap.add_argument("--tile-px", default="256,512,1024,2048,4096")
    ap.add_argument("--bands", default="1,4,13")
    ap.add_argument("--dtypes", default="uint8,int16,float32")
    ap.add_argument("--srids", default="4326,3857,32618,27700")
    ap.add_argument("--nodata-frac", default="0.02")
    ap.add_argument("--row-rows", type=int, default=10000)
    ap.add_argument("--row-tile-px", type=int, default=1024)
    ap.add_argument("--row-bands", type=int, default=4)
    ap.add_argument("--row-dtype", default="float32")
    ap.add_argument("--nodata-warn-threshold", type=float, default=0.9)
    a = ap.parse_args(argv)

    corpus = generate_corpus(
        out_dir=a.out,
        seed=a.seed,
        tile_px=_parse_int_list(a.tile_px),
        bands=_parse_int_list(a.bands),
        dtypes=a.dtypes.split(","),
        srids=_parse_int_list(a.srids),
        nodata_fracs=_parse_float_list(a.nodata_frac),
        row_rows=a.row_rows,
        row_tile_px=a.row_tile_px,
        row_bands=a.row_bands,
        row_dtype=a.row_dtype,
    )
    problems = validity_gate(a.out, corpus, a.nodata_warn_threshold)
    if problems:
        print("VALIDITY GATE FAILED:")
        for p in problems:
            print("  -", p)
        raise SystemExit(1)
    # Materialize the C3 (tile_array) synth tiles NOW, while still in the pyrx
    # venv. gbx:bench:all runs the heavyweight leg first, so the pyrx runner's
    # lazy synthesis would be too late; writing them here (at gen-data) means both
    # engines later READ identical pre-existing files via the same path math, and
    # a standalone gbx:bench:heavyweight run also finds them.
    from databricks.labs.gbx.bench import synth as _synth

    synth_files = _synth.materialize_all(a.out, corpus)
    print(
        json.dumps(
            {
                "tiles_size_sweep": len(corpus.size_sweep),
                "tiles_row_pool": len(corpus.row_pool.tiles),
                "synth_files": len(synth_files),
                "out": a.out,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
