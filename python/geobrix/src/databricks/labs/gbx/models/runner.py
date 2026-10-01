"""GPU-aware chip -> infer -> stitch -> polygonize orchestration for gbx.models.

``segment_raster`` is the single GeoBrix call the ortho-segmentation notebook makes: it
chips a raster into an (optionally overlapping) grid of tiles, runs an injectable
segmenter across 1..N GPU device slots via ``pyrx.core.gpu_pool.gpu_pool_map``,
vectorizes each tile's label mask into georeferenced polygons via
``pyrx.core.features.polygonize``, and merges polygon fragments that straddle a tile
seam into one polygon per real-world object before dropping anything under
``min_area``.

Seam-merge relies on georeferencing, not bookkeeping: when ``overlap`` > 0, an object
split by a seam is captured by two (or more) neighboring windows, and because each
window's fragment is vectorized with its own true ``ds.window_transform``, the two
fragments land at their real-world footprint -- which necessarily overlaps wherever the
windows themselves overlap. Grouping fragments by plain geometric intersection and
unioning each group is therefore sufficient; no separate "touch within overlap"
distance heuristic is needed on top of correct per-tile georeferencing.

Light-only module: torch is imported lazily, and only when ``gpus="all"`` needs to
probe ``torch.cuda.device_count()``. Passing an int ``gpus`` value never imports torch,
so this module stays importable (and Serverless-safe: no ``spark.conf``/``_jvm``/
``.rdd``/``sparkContext``) with no GPU deps installed.
"""

from contextlib import contextmanager
from threading import Lock

import numpy as np
import pandas as pd
import rasterio
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.windows import Window
from shapely.ops import unary_union

from databricks.labs.gbx.core.crs import area_m2 as _area_m2
from databricks.labs.gbx.pyrx.core import features, tiling
from databricks.labs.gbx.pyrx.core.gpu_pool import gpu_pool_map


def _resolve_gpus(gpus) -> int:
    """Normalize the ``gpus`` argument to a device count.

    An int value passes through untouched and NEVER imports torch -- a caller that
    already knows how many device slots to use gets no CUDA probe. Only the string
    ``"all"`` triggers the lazy ``torch.cuda.device_count()`` import+probe.
    """
    if isinstance(gpus, bool) or not isinstance(gpus, (int, str)):
        raise ValueError(
            f"segment_raster: gpus must be 'all' or a positive int, got {gpus!r}"
        )
    if isinstance(gpus, int):
        if gpus < 1:
            raise ValueError(f"segment_raster: gpus must be >= 1, got {gpus}")
        return gpus
    if gpus == "all":
        import torch  # lazy: only "all" probes CUDA; an int gpus value never imports torch

        return max(1, torch.cuda.device_count())
    raise ValueError(
        f"segment_raster: gpus must be 'all' or a positive int, got {gpus!r}"
    )


def _is_open_dataset(obj) -> bool:
    return hasattr(obj, "read") and hasattr(obj, "transform") and hasattr(obj, "crs")


@contextmanager
def _open_input(tile_or_path):
    """Normalize ``tile_or_path`` (an open rasterio dataset, raw GTiff bytes, or a file
    path) to an open rasterio dataset. An already-open dataset is yielded as-is -- the
    caller owns its lifetime; bytes/paths are opened here and closed on exit."""
    if _is_open_dataset(tile_or_path):
        yield tile_or_path
        return
    if isinstance(tile_or_path, (bytes, bytearray)):
        with MemoryFile(bytes(tile_or_path)) as mf, mf.open() as ds:
            yield ds
        return
    with rasterio.open(tile_or_path) as ds:
        yield ds


def _iter_chips(ds, tile_px, overlap):
    """Yield ``(array, transform)`` chips for the grid ``tiling.plan_grid_windows``
    plans over ``ds``.

    This is the "thin overlapping-window adapter" R1 calls for -- ``iter_overlapping``
    does not exist on ``tiling``. ``plan_grid_windows`` (pure window planning, no
    dataset/bytes) already carries the correct overlap-PERCENTAGE contract shared with
    heavy ``rst_tooverlappingtiles``; this just turns each planned window into the
    (array, transform) pair a segmenter + polygonize need, mirroring the
    Window/``ds.read``/``ds.window_transform`` idiom ``tiling._iter_window_tiles`` already
    uses (skipping its GTiff-byte encode step, which we don't need here).
    """
    for col, row, w, h in tiling.plan_grid_windows(
        ds.width, ds.height, tile_px, tile_px, overlap
    ):
        win = Window(col, row, w, h)
        yield ds.read(window=win), ds.window_transform(win)


def _polygonize_mask(mask, transform, crs):
    """Wrap a plain HxW label mask into a scratch raster and vectorize it via
    ``features.polygonize`` (R1: reuse the existing vectorizer; this is the thin
    adapter that gives it a raster to read).

    Returns ``(wkb, value)`` pairs for every label > 0. Label 0 (background) is
    dropped here -- rather than via a NoData mask on the scratch raster -- so an
    all-zero (no-object) mask cleanly yields zero fragments without a spurious
    whole-tile background polygon leaking through.
    """
    mask = np.asarray(mask).astype("int32")
    profile = dict(
        driver="GTiff",
        width=mask.shape[1],
        height=mask.shape[0],
        count=1,
        dtype="int32",
        transform=transform,
        crs=crs,
    )
    # EXEMPT: ephemeral scratch raster -- written only to hand a mask to
    # features.polygonize() and discarded a few lines later; never persisted or
    # returned to a caller, so the compression.py creation-option policy does not
    # apply (mirrors the measurement-only exemption on tiling._encoded_size_bytes).
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(mask, 1)
        with mf.open() as src:
            frags = features.polygonize(src, band=1, connectedness=4)
    return [(wkb, value) for wkb, value in frags if value != 0]


# _area_m2: thin re-export of core.crs.area_m2 (imported above), kept as a private
# name here so existing call sites and tests in this module are unchanged. The
# geodesic/planar area helper now lives in core.crs, shared with rst_land_cover's
# metric coverage/min_area needs.


def _merge_seams(frags, min_area, crs):
    """Union polygon fragments that geometrically touch/overlap into one polygon per
    connected group (see module docstring for why plain geometric intersection is a
    sufficient seam-merge criterion here), then drop groups whose merged area (in
    SQUARE METERS -- see ``_area_m2``; the polygons are in ``crs``) is under
    ``min_area``. Returns a list of ``{"label", "geom", "score"}`` row dicts.
    """
    geoms = [shapely.wkb.loads(bytes(wkb)) for wkb, _ in frags]
    n = len(geoms)
    parent = list(range(n))

    def find(i):
        while parent[i] != i:
            i = parent[i]
        return i

    def union(i, j):
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[ri] = rj

    for i in range(n):
        for j in range(i + 1, n):
            if geoms[i].intersects(geoms[j]):
                union(i, j)

    groups: dict = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(geoms[i])

    rows = []
    label = 0
    for group_geoms in groups.values():
        merged = group_geoms[0] if len(group_geoms) == 1 else unary_union(group_geoms)
        if _area_m2(merged, crs) < min_area:
            continue
        rows.append({"label": label, "geom": shapely.wkb.dumps(merged), "score": 1.0})
        label += 1
    return rows


def _default_geosam_factory(gpu_id: int):
    """Per-device GeoSAM handle builder -- the default (``segmenter is None``)
    ``segment_raster`` factory. Binds inference to device ``gpu_id`` (``torch.cuda.
    set_device`` + ``load_geosam(device=f"cuda:{gpu_id}")``) and returns a
    ``(image) -> mask`` callable closed over that device-bound handle.

    ``segment_raster`` calls this at most ONCE per distinct ``gpu_id`` (caches the
    returned callable in a per-call dict), so a device's SamGeo predictor -- which is
    stateful and not safe to call concurrently -- is never shared across devices and
    never rebuilt per chip. ``gpu_pool_map`` already guarantees at most one chip runs
    on a given device at a time, so a per-device handle is never invoked concurrently
    either.

    A module-level function (rather than a closure inside ``segment_raster``) so tests
    can ``monkeypatch.setattr(runner, "_default_geosam_factory", fake)`` to exercise the
    per-device plumbing without torch/GPU installed.

    torch is imported lazily here, inside the default factory -- which only ever runs
    on a GPU worker taking the ``segmenter=None`` path -- so the light wheel stays
    importable with no torch installed.
    """
    import torch

    from databricks.labs.gbx.models.geosam import load_geosam
    from databricks.labs.gbx.models.geosam import segment as _geosam_segment

    torch.cuda.set_device(gpu_id)
    handle = load_geosam(device=f"cuda:{gpu_id}")

    def _segment(image):
        return _geosam_segment(handle, image)

    return _segment


def segment_raster(
    tile_or_path,
    *,
    segmenter=None,
    model: str = "geosam",
    gpus="all",
    tile_px: int = 1024,
    overlap: int = 6,
    min_area: float = 10.0,
) -> pd.DataFrame:
    """Chip -> infer -> stitch -> polygonize: the single GeoBrix call the
    ortho-segmentation notebook makes.

    Chips ``tile_or_path`` into an ``overlap``-percent grid of ``tile_px`` x
    ``tile_px`` windows (``pyrx.core.tiling.plan_grid_windows`` -- the same
    overlap-percentage contract as heavy ``rst_tooverlappingtiles``), runs
    ``segmenter`` (or a GeoSAM handle when ``segmenter`` is ``None``) across 1..N GPU
    device slots via ``gpu_pool_map``, vectorizes each tile's label mask into
    georeferenced polygons, and merges fragments that straddle a seam into one
    polygon per real-world object before dropping anything under ``min_area``.

    Args:
        tile_or_path: an open rasterio dataset, raw GTiff bytes, or a file path.
        segmenter: injectable ``(image: HxWx3 uint8) -> mask: HxW int`` callable.
            Device-agnostic -- every chip, on every GPU device slot, calls the SAME
            ``segmenter``. Defaults to a per-device GeoSAM handle (``model="geosam"``,
            the only backend wired up so far) when ``None``. Tests inject a fake.
        model: which built-in backend to default to when ``segmenter`` is ``None``.
            Only ``"geosam"`` is implemented today.
        gpus: ``"all"`` probes ``torch.cuda.device_count()`` (lazy import); an int
            passes through untouched and never imports torch.
        tile_px: square tile size in pixels.
        overlap: seam overlap as a PERCENTAGE of ``tile_px`` -- 0-100, matching
            ``tiling.plan_grid_windows`` / heavy ``rst_tooverlappingtiles`` -- not a
            pixel count. E.g. ``overlap=6`` on ``tile_px=1024`` is a ~61px seam.
        min_area: minimum object area, in SQUARE METERS -- polygons (post seam-merge)
            smaller than this are dropped. Measured metrically regardless of the source
            raster's CRS (geodesic area for a geographic CRS such as EPSG:4326, planar
            for a projected one -- see ``_area_m2``), so the threshold means the same
            real-world size whether the COG is in degrees or meters.

    Returns:
        ``pandas.DataFrame`` with columns ``label:int``, ``geom:bytes`` (WKB),
        ``score:float``. ``score`` is currently a constant ``1.0`` -- a bare label
        mask carries no per-object confidence; wire a real score through once a
        confidence-scored backend exists.
    """
    if segmenter is None:
        if model != "geosam":
            raise ValueError(
                f"segment_raster: unsupported model {model!r}; only 'geosam' has a "
                "built-in segmenter today -- pass segmenter= explicitly for anything else"
            )
        # Per-device factory: segment_raster (below) calls this at most once per
        # distinct gpu_id and caches the result, so the default path binds inference to
        # the chip's actual assigned device instead of one shared cuda:0 handle.
        factory = _default_geosam_factory
    else:
        # Injected segmenter is device-agnostic: every gpu_id gets the same callable.
        def factory(_gpu_id):
            return segmenter

    n_gpus = _resolve_gpus(gpus)

    with _open_input(tile_or_path) as ds:
        crs = ds.crs
        chips = list(_iter_chips(ds, tile_px, overlap))

    # Per-gpu_id handle cache: gpu_pool_map guarantees at most one chip runs on a given
    # device at a time, so a cached per-device handle (e.g. a stateful SamGeo predictor)
    # is never invoked concurrently. The lock guards only the check-and-build of a new
    # cache entry (paid once per distinct device, never per chip) -- the inference call
    # itself (`fn(image)`) runs outside the lock, so once every device's handle is
    # built, chips on different devices still run concurrently.
    handles: dict = {}
    handles_lock = Lock()

    def _run_one(chip, gpu_id):
        arr, transform = chip
        image = np.moveaxis(arr, 0, -1)
        with handles_lock:
            fn = handles.get(gpu_id)
            if fn is None:
                fn = factory(gpu_id)
                handles[gpu_id] = fn
        mask = fn(image)
        return _polygonize_mask(mask, transform, crs)

    per_chip = gpu_pool_map(chips, _run_one, gpus=max(1, n_gpus))
    all_frags = [frag for chip_frags in per_chip for frag in chip_frags]
    rows = _merge_seams(all_frags, min_area, crs)
    return pd.DataFrame(rows, columns=["label", "geom", "score"])
