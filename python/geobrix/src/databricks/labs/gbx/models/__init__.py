"""gbx.models — model-backed inference for GeoBrix rasters (GeoSAM segmentation,
Unity Catalog model serving).

Light-only package: importing ``databricks.labs.gbx.models`` or any of its submodules
never imports torch/segment-geospatial. Those land only when a GPU-backed model is
actually loaded (``load_geosam``) or served, so the base light wheel stays importable
with no GPU deps installed. Install GPU deps via the ``geobrix[models_gpu_env5]`` extra.

PEP 562 lazy exports: ``load_geosam``/``segment`` (geosam.py), ``segment_raster``
(runner.py), ``register_to_unity_gateway``/``create_endpoint``/``query`` (serving.py).
"""

__all__ = [
    "load_geosam",
    "segment",
    "segment_raster",
    "register_to_unity_gateway",
    "create_endpoint",
    "query",
]


def __getattr__(name):
    if name == "load_geosam":
        from databricks.labs.gbx.models.geosam import load_geosam

        return load_geosam
    if name == "segment":
        from databricks.labs.gbx.models.geosam import segment

        return segment
    if name == "segment_raster":
        from databricks.labs.gbx.models.runner import segment_raster

        return segment_raster
    if name == "register_to_unity_gateway":
        from databricks.labs.gbx.models.serving import register_to_unity_gateway

        return register_to_unity_gateway
    if name == "create_endpoint":
        from databricks.labs.gbx.models.serving import create_endpoint

        return create_endpoint
    if name == "query":
        from databricks.labs.gbx.models.serving import query

        return query
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
