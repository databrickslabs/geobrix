"""GPU-aware chip -> infer -> stitch -> polygonize orchestration for gbx.models.

``segment_raster`` is the single GeoBrix call the ortho-segmentation notebook makes: it
chips a raster into an (optionally overlapping) grid of tiles, runs an injectable
segmenter across 1..N GPU device slots via ``pyrx.core.gpu_pool.gpu_pool_map``,
vectorizes each tile's label mask into georeferenced polygons via
``pyrx.core.features.polygonize``, and merges polygon fragments that straddle a tile
seam into one polygon per real-world object before dropping anything under
``min_area``.

Seam-merge merges only the pieces of ONE object that a chip seam cut apart. Plain
geometric intersection is not enough: SAM's automatic masks tile nearly the whole scene, so
neighbouring objects touch, and grouping by ``intersects`` chains them into one blob. Each
fragment therefore carries the id of the chip it came from, and two fragments are merged
only when (a) they come from DIFFERENT chips and (b) they agree across the seam between
those chips:

* chips that overlap (``overlap`` > 0): each window's fragment is vectorized with its own
  true ``ds.window_transform``, so an object both windows see lands at the same real-world
  footprint inside their overlap zone. The two fragments are merged when, clipped to that
  zone, their intersection-over-union is at least ``_SEAM_IOU``;
* chips that only abut (``overlap`` = 0): the fragments must share a boundary segment ALONG
  the seam line, at least a pixel long and covering ``_SEAM_IOU`` of their seam edges. A
  shared edge anywhere else, or a single shared point, is not a seam.

Fragments of one chip are never merged with each other: within a chip the segmenter's own
labels are the objects (the pieces of one label are one object).

NoData is masked before polygonizing: a pixel the raster marks invalid (NoData, its mask
band, or alpha 0; ``pyrx.core._nodata.valid_mask``) -- or, for a raster that declares none
of them, a pixel that is 0 in every band -- is background in every chip's label mask, so a
black or transparent border never becomes an object.

Light-only module: torch is imported lazily, and only when ``gpus="all"`` needs to
probe ``torch.cuda.device_count()``. Passing an int ``gpus`` value never imports torch,
so this module stays importable (and Serverless-safe: no ``spark.conf``/``_jvm``/
``.rdd``/``sparkContext``) with no GPU deps installed.
"""

import os
from contextlib import contextmanager
from threading import Lock

import numpy as np
import pandas as pd
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.windows import Window
from shapely.geometry import Polygon
from shapely.ops import unary_union
from shapely.strtree import STRtree

from databricks.labs.gbx._gdal_policy import open_bytes, open_data
from databricks.labs.gbx.core.crs import area_m2 as _area_m2
from databricks.labs.gbx.ds._listing import to_local_path
from databricks.labs.gbx.pyrx.core import _nodata, features, tiling
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
        with MemoryFile(bytes(tile_or_path)) as mf, open_bytes(mf) as ds:
            yield ds
        return
    with open_data(tile_or_path, remote_ok=True) as ds:  # the caller's own path
        yield ds


def _declares_validity(ds) -> bool:
    """True when ``ds`` says which pixels are NoData: a NoData value, a mask band or an
    alpha band (any band whose GDAL mask is not ``all_valid``)."""
    from rasterio.enums import MaskFlags

    return _nodata.has_alpha(ds) or any(
        MaskFlags.all_valid not in flags for flags in ds.mask_flag_enums
    )


def _iter_chips(ds, tile_px, overlap):
    """Yield ``(image, transform, valid, footprint)`` chips for the grid
    ``tiling.plan_grid_windows`` plans over ``ds``.

    This is the "thin overlapping-window adapter" R1 calls for -- ``iter_overlapping``
    does not exist on ``tiling``. ``plan_grid_windows`` (pure window planning, no
    dataset/bytes) already carries the correct overlap-PERCENTAGE contract shared with
    heavy ``rst_tooverlappingtiles``; this just turns each planned window into what a
    segmenter + polygonize need, mirroring the Window/``ds.read``/``ds.window_transform``
    idiom ``tiling._iter_window_tiles`` already uses (skipping its GTiff-byte encode step,
    which we don't need here).

    ``image`` is the window's data bands (the alpha band, when ``ds`` has one, is left
    out: the segmenter takes color bands); ``valid`` is the window's HxW validity mask
    (see the module docstring); ``footprint`` is the window's real-world polygon in
    ``ds.crs``, which the seam merge uses to find the seam between two chips.
    """
    declared = _declares_validity(ds)
    alpha = _nodata.alpha_index(ds.colorinterp)
    data_bands = [i + 1 for i in range(ds.count) if i != alpha]
    for col, row, w, h in tiling.plan_grid_windows(
        ds.width, ds.height, tile_px, tile_px, overlap
    ):
        win = Window(col, row, w, h)
        arr = ds.read(data_bands, window=win)
        if declared:
            valid = _nodata.valid_mask(ds, window=win)
        else:
            valid = np.any(arr != 0, axis=0)
        transform = ds.window_transform(win)
        footprint = Polygon(
            [
                transform * (0, 0),
                transform * (w, 0),
                transform * (w, h),
                transform * (0, h),
            ]
        )
        yield arr, transform, valid, footprint


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
        with open_bytes(mf) as src:
            frags = features.polygonize(src, band=1, connectedness=4)
    return [(wkb, value) for wkb, value in frags if value != 0]


# _area_m2: thin re-export of core.crs.area_m2 (imported above), kept as a private
# name here so existing call sites and tests in this module are unchanged. The
# geodesic/planar area helper now lives in core.crs, shared with rst_land_cover's
# metric coverage/min_area needs.


# The agreement two fragments from neighbouring chips need across their seam to be one
# object: the intersection-over-union of the two, clipped to the chips' overlap zone (or,
# for abutting chips, of their edges along the seam line).
_SEAM_IOU = 0.5


def _seam_match(a, b, zone, px) -> bool:
    """True when fragments ``a`` and ``b`` (from two different chips whose footprints meet
    in ``zone``) are the two sides of one object cut by the seam -- see the module
    docstring. ``px`` is the pixel size in CRS units."""
    # Overlapping chips: compare the two fragments inside the overlap zone.
    if zone.area > 0:
        za, zb = a.intersection(zone), b.intersection(zone)
        inter = za.intersection(zb).area
        if inter <= 0:
            return False
        return inter / za.union(zb).area >= _SEAM_IOU
    if zone.length <= 0:  # chips that meet at a single corner point: no seam
        return False
    # Abutting chips: the seam is the line ``zone``. Each fragment's edge along it, as a
    # thin strip (tolerant to float noise between the two window transforms).
    tol = 0.05 * px
    band = zone.buffer(tol, cap_style="flat")
    sa, sb = a.buffer(tol).intersection(band), b.buffer(tol).intersection(band)
    shared = sa.intersection(sb).area
    # Less than a pixel of shared seam is a corner, not a seam.
    if shared / (2 * tol) < px:
        return False
    return shared / sa.union(sb).area >= _SEAM_IOU


def _merge_seams(frags, min_area, crs, footprints, px):
    """Merge the fragments of one object that chip seams cut apart, then drop objects whose
    merged area (in SQUARE METERS -- see ``_area_m2``; the polygons are in ``crs``) is under
    ``min_area``. Returns a list of ``{"label", "geom", "score"}`` row dicts.

    ``frags`` holds ``(wkb, value, chip)`` triples: ``chip`` indexes ``footprints`` (each
    chip's real-world polygon). The pieces of one label in one chip are one object; two
    objects are merged only across a seam between different chips (``_seam_match``), never
    within a chip. ``px`` is the raster's pixel size in CRS units.
    """
    # One object per (chip, label): the pieces of a label are the segmenter's own object.
    pieces: dict = {}
    for wkb, value, chip in frags:
        pieces.setdefault((chip, value), []).append(shapely.wkb.loads(bytes(wkb)))
    keys = list(pieces)
    geoms = [g[0] if len(g) == 1 else unary_union(g) for g in pieces.values()]
    chips = [chip for chip, _ in keys]
    n = len(geoms)
    parent = list(range(n))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    zones: dict = {}

    def zone_of(ci, cj):
        k = (min(ci, cj), max(ci, cj))
        if k not in zones:
            zones[k] = footprints[ci].intersection(footprints[cj])
        return zones[k]

    if n > 1:
        left, right = STRtree(geoms).query(geoms, predicate="intersects")
        for i, j in zip(left.tolist(), right.tolist()):
            if i >= j or chips[i] == chips[j]:
                continue
            if find(i) == find(j):
                continue
            if _seam_match(geoms[i], geoms[j], zone_of(chips[i], chips[j]), px):
                parent[find(i)] = find(j)

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


def _resolve_weights(weights, segmenter) -> str | None:
    """Validate ``weights`` and return its bare local path (``None`` stays ``None``).

    ``dbfs:/Volumes/...`` and ``file:`` forms are normalised through
    ``ds._listing.to_local_path``. Refused up front, on the driver and before any chip
    is cut or model is loaded: ``weights`` together with ``segmenter`` (an injected
    segmenter never loads a checkpoint, so ``weights`` would be silently ignored), and a
    path that is not a file.
    """
    if weights is None:
        return None
    if segmenter is not None:
        raise ValueError(
            "segment_raster: weights= and segmenter= cannot be used together; an "
            "injected segmenter loads no checkpoint, so weights= would be ignored. "
            "Fix: pass only one of them -- load the checkpoint inside your segmenter, "
            "or drop segmenter= to use the built-in GeoSAM with weights=."
        )
    local = to_local_path(os.fspath(weights))
    if not os.path.isfile(local):
        raise FileNotFoundError(
            f"segment_raster: GeoSAM checkpoint not found at {local!r} "
            f"(weights={weights!r}). Fix: stage the checkpoint file (for example "
            "sam_vit_h_4b8939.pth) on a Volume and pass its /Volumes/... path, or "
            "omit weights= to let SamGeo download it (needs internet egress)."
        )
    return local


def _default_geosam_factory(gpu_id: int, weights: str | None = None):
    """Per-device GeoSAM handle builder -- the default (``segmenter is None``)
    ``segment_raster`` factory. Binds inference to device ``gpu_id`` (``torch.cuda.
    set_device`` + ``load_geosam(weights=weights, device=f"cuda:{gpu_id}")``) and returns a
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

    ``weights`` is a bare local checkpoint path (already validated by
    ``_resolve_weights``) passed straight to ``load_geosam``; ``None`` lets SamGeo
    download its default checkpoint.

    torch is imported lazily here, inside the default factory -- which only ever runs
    on a GPU worker taking the ``segmenter=None`` path -- so the light wheel stays
    importable with no torch installed.
    """
    import torch

    from databricks.labs.gbx.models.geosam import load_geosam
    from databricks.labs.gbx.models.geosam import segment as _geosam_segment

    torch.cuda.set_device(gpu_id)
    handle = load_geosam(weights=weights, device=f"cuda:{gpu_id}")

    def _segment(image):
        return _geosam_segment(handle, image)

    return _segment


def segment_raster(
    tile_or_path,
    *,
    segmenter=None,
    weights=None,
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
        weights: optional path to a GeoSAM checkpoint file -- a local path, a
            ``/Volumes/...`` path or a ``dbfs:/Volumes/...`` URI. Passed to
            ``load_geosam(weights=...)``. ``None`` (default) lets SamGeo download
            ``sam_vit_h_4b8939.pth`` from its CDN, which needs internet egress; on a
            workspace without it, stage the checkpoint on a Volume and pass its path.
            Cannot be combined with ``segmenter``.
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
    local_weights = _resolve_weights(weights, segmenter)
    if segmenter is None:
        if model != "geosam":
            raise ValueError(
                f"segment_raster: unsupported model {model!r}; only 'geosam' has a "
                "built-in segmenter today -- pass segmenter= explicitly for anything else"
            )
        # Per-device factory: segment_raster (below) calls this at most once per
        # distinct gpu_id and caches the result, so the default path binds inference to
        # the chip's actual assigned device instead of one shared cuda:0 handle.
        if local_weights is None:
            factory = _default_geosam_factory
        else:

            def factory(gpu_id):
                return _default_geosam_factory(gpu_id, weights=local_weights)

    else:
        # Injected segmenter is device-agnostic: every gpu_id gets the same callable.
        def factory(_gpu_id):
            return segmenter

    n_gpus = _resolve_gpus(gpus)

    with _open_input(tile_or_path) as ds:
        crs = ds.crs
        t = ds.transform
        px = min(np.hypot(t.a, t.d), np.hypot(t.b, t.e))
        chips = [(i, *c) for i, c in enumerate(_iter_chips(ds, tile_px, overlap))]

    # Per-gpu_id handle cache: gpu_pool_map guarantees at most one chip runs on a given
    # device at a time, so a cached per-device handle (e.g. a stateful SamGeo predictor)
    # is never invoked concurrently. The lock guards only the check-and-build of a new
    # cache entry (paid once per distinct device, never per chip) -- the inference call
    # itself (`fn(image)`) runs outside the lock, so once every device's handle is
    # built, chips on different devices still run concurrently.
    handles: dict = {}
    handles_lock = Lock()

    def _run_one(chip, gpu_id):
        chip_id, arr, transform, valid, _ = chip
        image = np.moveaxis(arr, 0, -1)
        with handles_lock:
            fn = handles.get(gpu_id)
            if fn is None:
                fn = factory(gpu_id)
                handles[gpu_id] = fn
        # NoData is background before polygonizing: no mask over it becomes an object.
        mask = np.where(valid, np.asarray(fn(image)), 0)
        return [(wkb, v, chip_id) for wkb, v in _polygonize_mask(mask, transform, crs)]

    per_chip = gpu_pool_map(chips, _run_one, gpus=max(1, n_gpus))
    all_frags = [frag for chip_frags in per_chip for frag in chip_frags]
    footprints = [c[4] for c in chips]
    rows = _merge_seams(all_frags, min_area, crs, footprints, px)
    return pd.DataFrame(rows, columns=["label", "geom", "score"])
