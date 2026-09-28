"""GeoSAM (segment-geospatial / SamGeo) load + single-tile automatic-mask segmentation.

Light-only module: torch and segment-geospatial (``samgeo``) are imported lazily, inside
``_import_backend``, so importing ``databricks.labs.gbx.models.geosam`` never requires
GPU deps. Callers that invoke ``load_geosam`` without those deps installed get a clear
``ModelDepsMissing`` with an actionable install hint.
"""

from dataclasses import dataclass
from typing import Any


class ModelDepsMissing(ImportError):
    """Raised by load_geosam when torch/segment-geospatial are not installed."""


@dataclass
class GeoSamHandle:
    backend: Any
    model_type: str


def _import_backend(model_type: str, weights: str | None, device: str):
    from samgeo import SamGeo  # segment-geospatial

    # NOTE (confirm exact SamGeo kwargs against segment-geospatial docs at impl time):
    return SamGeo(
        model_type=model_type, checkpoint=weights, device=device, automatic=True
    )


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
    """HxWx3 uint8 -> HxW int32 label mask (0=background)."""
    return handle.backend.generate(image)
