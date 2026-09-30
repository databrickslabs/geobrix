"""GeoSAM (segment-geospatial / SamGeo) load + single-tile automatic-mask segmentation.

Light-only module: torch and segment-geospatial (``samgeo``) are imported lazily, inside
``_import_backend``, so importing ``databricks.labs.gbx.models.geosam`` never requires
GPU deps. Callers that invoke ``load_geosam`` without those deps installed get a clear
``ModelDepsMissing`` with an actionable install hint.
"""

from dataclasses import dataclass
from typing import Any

import numpy as np


class ModelDepsMissing(ImportError):
    """Raised by load_geosam when torch/segment-geospatial are not installed."""


@dataclass
class GeoSamHandle:
    backend: Any
    model_type: str


def _import_backend(model_type: str, weights: str | None, device: str):
    from samgeo import SamGeo  # segment-geospatial

    # Real SamGeo.__init__(model_type, automatic, device, checkpoint_dir, sam_kwargs,
    # **kwargs) has NO formal `checkpoint` param -- a specific checkpoint only reaches
    # it via **kwargs, and the real code does `os.path.exists(checkpoint)` on it. A
    # bare `checkpoint=None` (the old bug) makes that a TypeError before download can
    # even run. When `weights` is None, omit the kwarg entirely so SamGeo falls
    # through to its own checkpoint_dir-based auto-download; when `weights` is a real
    # path, `os.path.exists` succeeds and that exact file is used as-is.
    kwargs = {"checkpoint": weights} if weights is not None else {}
    return SamGeo(model_type=model_type, device=device, automatic=True, **kwargs)


def load_geosam(
    *, model_type: str = "vit_h", weights: str | None = None, device: str = "cuda"
) -> GeoSamHandle:
    # `_import_backend` is a plain module-level function (not a method) precisely so
    # tests can monkeypatch it to simulate a missing-deps ImportError without needing
    # torch/samgeo installed; the conversion to ModelDepsMissing happens here so both
    # the real `from samgeo import SamGeo` failure and a mocked one are covered.
    try:
        backend = _import_backend(model_type, weights, device)
    except ImportError as e:
        raise ModelDepsMissing(
            "GeoSAM requires the model deps — install geobrix[models_gpu_env5]"
        ) from e
    return GeoSamHandle(backend=backend, model_type=model_type)


def segment(handle: GeoSamHandle, image):
    """HxWx3 uint8 -> HxW int32 label mask (0=background).

    The real ``SamGeo.generate(source, ...)`` performs automatic mask generation but
    RETURNS None -- results land on the instance, not the return value.
    ``generate`` accepts ``source`` as either a file path or an in-memory HxWxC uint8
    array (``isinstance(source, np.ndarray)`` is handled directly, via
    ``prepare_image_for_sam`` with its default HWC ``channel_axis=-1``) -- so the
    already-in-memory ``image`` this function receives is passed straight through,
    no temp file needed. With ``output=None`` (the default) it skips writing to disk
    and leaves the per-object label mask on ``handle.backend.objects``: a 2D array
    (dtype sized to the object count) with 0=background and a unique positive
    integer per object when ``unique=True`` (the default) -- exactly the label-mask
    contract this function returns.

    On an all-background chip (common when chipping a large orthomosaic) SamGeo may
    leave ``handle.backend.objects`` as None, or an empty/non-2D array, instead of a
    proper HxW label mask. Treat that as all-background: return an HxW int32 zeros
    mask sized from the INPUT image, rather than letting
    ``np.asarray(None).astype("int32")`` produce an invalid 0-d array.
    """
    handle.backend.generate(image, foreground=True, unique=True)
    objs = handle.backend.objects
    if objs is None:
        return np.zeros(image.shape[:2], dtype=np.int32)
    arr = np.asarray(objs)
    if arr.ndim != 2 or arr.size == 0:
        return np.zeros(image.shape[:2], dtype=np.int32)
    return arr.astype("int32")
