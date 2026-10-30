"""Deterministic multi-tile input synthesis for the `tile_array` bench adapter.

The C3 multi-tile functions (rst_frombands / rst_combineavg / rst_merge) each
consume an ARRAY of tiles, but a corpus row carries ONE tile. This module derives
the multi-tile input from that single source and writes it to disk ONCE, so BOTH
benchmark engines (pyrx + heavy) read byte-identical input files.

Write-once-read-both is the safest cross-engine input-identity guarantee: rather
than each engine re-synthesizing (and risking subtly different GTiff encodings),
the pyrx runner synthesizes the files, and the heavy runner reads the SAME paths.
Both engines locate them via the deterministic ``synth_dir`` (computed identically
from corpus root + tile path + fn), so no coordination channel is needed beyond
agreeing on the path math.

Synthesis recipes (deterministic, idempotent):
  * ``frombands``  -> split the N-band source into N single-band GTiffs (N paths,
                      in band order; element i carries source band i).
  * ``combineavg`` -> 2 ALIGNED copies of the source (identical
                      transform/shape/CRS) so the per-pixel mean is well-defined.
  * ``merge``      -> 2 copies with OFFSET geotransform origins (origin shifted by
                      half the tile width/height) so their extents tile into a
                      union for the mosaic.

Idempotent: a given (source, fn) always writes to the same paths with the same
bytes; if the outputs already exist AND read back (first block) they are reused,
an unreadable one is rewritten.

Volume-safe: GDAL finishes a GTiff by seeking back to write the first IFD offset,
which a UC Volume (FUSE) does not honour -- the file is left with IFD offset 0 and
is unreadable. So every file is written to local disk, then copied sequentially to
a hidden sibling of the destination and renamed into place (a reader never sees a
partial file).
"""

from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from pathlib import Path
from typing import List, Union

from rasterio.transform import Affine

from databricks.labs.gbx._gdal_policy import open_data
from databricks.labs.gbx.bench import manifest as _m
from databricks.labs.gbx.pyrx.core.compression import source_profile
from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_dir

# Recipes and their output filenames. The element ORDER is the band/merge order
# the consuming function relies on (frombands element 0 -> band 1, etc.).
_RECIPES = ("frombands", "combineavg", "merge")


def synth_dir(corpus_root, tile_rel_path, fn: str) -> str:
    """Deterministic output dir for a (corpus tile, fn) synthesis.

    Both engines compute this identically from the corpus root, the tile's
    corpus-relative path, and the function name, so the heavy runner reads the
    exact files the pyrx runner wrote. Lives UNDER the corpus root so it travels
    with the corpus and is cleaned up with it.
    """
    # A short stable hash of the tile path keeps the dir name filesystem-safe and
    # collision-free across nested corpus layouts, while staying deterministic.
    stem = hashlib.sha1(str(tile_rel_path).encode("utf-8")).hexdigest()[:12]
    return str(Path(corpus_root) / "_synth" / fn / stem)


def synthesize(src_path: str, fn: str, out_dir: str) -> List[str]:
    """Synthesize the multi-tile input for ``fn`` from ``src_path``.

    Returns the list of output GTiff paths in consumption order. Writes the files
    once; if they already exist (idempotent re-run) they are returned as-is.
    """
    if fn not in _RECIPES:
        raise ValueError(f"unknown synth recipe: {fn}")
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    if fn == "frombands":
        return _synth_frombands(src_path, out)
    if fn == "combineavg":
        return _synth_combineavg(src_path, out)
    return _synth_merge(src_path, out)


def publish_file(local: Union[str, Path], dest: Union[str, Path]) -> None:
    """Copy local file ``local`` to ``dest`` sequentially, atomically.

    Copies to a hidden sibling then ``os.replace``; if the filesystem refuses the
    replace, falls back to a direct copy (and cleans the sibling up).
    """
    dest = Path(dest)
    tmp = dest.with_name(f".{dest.name}.{uuid.uuid4().hex[:8]}.part")
    try:
        shutil.copyfile(
            local, tmp
        )  # copyfile: no chmod, which a Volume's FUSE mount may refuse
        os.replace(tmp, dest)
    except OSError as exc:
        print(f"publish_file: rename onto {dest} refused ({exc}); copying in place")
        shutil.copyfile(local, dest)
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


def _readable(path: Path) -> bool:
    """True when ``path`` exists and opens through ``open_data`` with a readable first block."""
    if not path.exists():
        return False
    try:
        with open_data(path) as ds:
            window = next(iter(ds.block_windows(1)))[1]
            ds.read(1, window=window)
        return True
    except Exception:  # noqa: BLE001 - any failure to open/read means rewrite
        return False


def _write(profile: dict, bands: list, path: Path) -> None:
    """Write ``bands`` (list of 2-D arrays) to ``path`` as a GTiff (local, then published)."""
    scratch = new_local_temp_dir("gbx_synth_")
    try:
        local = os.path.join(scratch, path.name)
        with open_data(local, "w", **profile) as dst:
            for i, arr in enumerate(bands, start=1):
                dst.write(arr, i)
        publish_file(local, path)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def _synth_frombands(src_path: str, out: Path) -> List[str]:
    """Split the N-band source into N single-band GTiffs (band order preserved)."""
    paths: List[str] = []
    with open_data(src_path) as ds:
        n = ds.count
        for b in range(1, n + 1):
            p = out / f"band_{b:02d}.tif"
            paths.append(str(p))
            if _readable(p):
                continue
            profile = source_profile(ds.profile, unless_jpeg=True)
            profile.update(driver="GTiff", count=1)
            _write(profile, [ds.read(b)], p)
    return paths


def _synth_combineavg(src_path: str, out: Path) -> List[str]:
    """Two ALIGNED copies of the source (identical grid) for a per-pixel mean."""
    paths: List[str] = []
    with open_data(src_path) as ds:
        data = ds.read()
        profile = source_profile(ds.profile, unless_jpeg=True)
        profile.update(driver="GTiff")
        for i in range(2):
            p = out / f"copy_{i}.tif"
            paths.append(str(p))
            if _readable(p):
                continue
            _write(profile, [data[b] for b in range(data.shape[0])], p)
    return paths


def _synth_merge(src_path: str, out: Path) -> List[str]:
    """Two copies with OFFSET origins so their extents tile into a union mosaic.

    The first copy keeps the source geotransform; the second shifts its origin by
    half the tile width (east) and half the tile height (south) so the two
    extents are distinct and abut/overlap into a larger union. The pixel size and
    CRS are unchanged so both copies snap onto one common grid for the mosaic.
    """
    paths: List[str] = []
    with open_data(src_path) as ds:
        data = ds.read()
        base = ds.transform
        w, h = ds.width, ds.height
        # half-extent offsets in CRS units (a >= 0 east shift, a downward south
        # shift since pixel height base.e is negative for north-up rasters).
        dx = base.a * (w / 2.0)
        dy = base.e * (h / 2.0)
        offset = Affine(base.a, base.b, base.c + dx, base.d, base.e, base.f + dy)
        for i, tr in enumerate((base, offset)):
            p = out / f"part_{i}.tif"
            paths.append(str(p))
            if _readable(p):
                continue
            profile = source_profile(ds.profile, unless_jpeg=True)
            profile.update(driver="GTiff", transform=tr)
            _write(profile, [data[b] for b in range(data.shape[0])], p)
    return paths


def _synth_source_tiles(corpus) -> List[str]:
    """Corpus-relative tile paths the C3 (tile_array) adapters synthesize from.

    Mirrors what both runners feed to ``synth_dir``:
      * pure-core path -> EVERY ``size_sweep`` tile (runner iterates ``te.path``);
      * spark-path     -> the first ``row_pool`` tile (``_array_root``).
    Materializing this exact set (deduped) means every path either engine later
    computes is already on disk before the heavyweight leg runs.
    """
    rels = [te.path for te in corpus.size_sweep]
    if corpus.row_pool.tiles:
        rels.append(corpus.row_pool.tiles[0].path)
    # dedupe while preserving order
    seen = set()
    ordered = []
    for r in rels:
        if r not in seen:
            seen.add(r)
            ordered.append(r)
    return ordered


def materialize_all(corpus_root, corpus=None) -> List[str]:
    """Materialize ALL C3 synth tiles into the corpus, once, before any run.

    The heavyweight leg of ``gbx:bench:all`` runs FIRST (before the pyrx leg), so
    relying on the pyrx runner to lazily synthesize the multi-tile inputs is too
    late: heavy ``rst_frombands``/``rst_combineavg``/``rst_merge`` would find no
    files. Materializing here (at gen-data time) writes the synth tiles for every
    corpus source tile x every C3 recipe, so BOTH engines just READ pre-existing,
    byte-identical files via the deterministic ``synth_dir`` path math.

    Idempotent: ``synthesize`` skips outputs that already exist, so re-running is
    a no-op. ``corpus`` defaults to reading ``corpus.json`` under ``corpus_root``.
    Returns the flat list of all written/existing synth file paths.
    """
    root = Path(corpus_root)
    if corpus is None:
        corpus = _m.Corpus.read(root / "corpus.json")
    written: List[str] = []
    for tile_rel in _synth_source_tiles(corpus):
        src = str(root / tile_rel)
        for recipe in _RECIPES:
            out_dir = synth_dir(corpus_root, tile_rel, recipe)
            written.extend(synthesize(src, recipe, out_dir))
    return written
