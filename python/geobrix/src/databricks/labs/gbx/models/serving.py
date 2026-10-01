"""GeoSAM MLflow pyfunc + Unity Catalog registration + GPU serving endpoint ops.

``build_geosam_pyfunc`` wraps ``gbx.models.runner.segment_raster`` in an mlflow
``PythonModel`` whose ``predict`` decodes a base64-encoded image, segments it, and
returns a GeoJSON ``FeatureCollection`` string. ``register_to_unity_gateway`` logs that
pyfunc to the Unity Catalog model registry using mlflow's private-wheel pattern: the
geobrix wheel is bundled INTO the model via ``code_paths`` (mlflow copies it to
``code/<basename>``) and ``pip_requirements`` installs geobrix from that bundled copy
(via the ``models_gpu_env5`` extra) + segment-geospatial -- a deliberate
usage-affiliation goal: the served model's environment carries geobrix. A
``file:///Volumes/...`` pip requirement does NOT work here: the Model Serving
container build has no access to the workspace's UC Volumes, so geobrix must ride
along as bundled model code instead. ``create_endpoint``/``query`` are thin wrappers
over the Databricks Model Serving API (see the ``databricks-model-serving`` skill).

RULING R4 -- mlflow is ABSENT from the light CI env (not in
``requirements-light-env6-ci.txt``, confirmed not importable in the geobrix-dev
container either). This module MUST import cleanly with no mlflow installed, so every
``import mlflow*`` is lazy, scoped inside the function that needs it -- including any
``mlflow.pyfunc.PythonModel`` subclass, which is defined *inside* ``build_geosam_pyfunc``
rather than at module top level.

To keep the predict LOGIC unit-testable without mlflow, it lives in a plain
``_predict_geojson`` helper (base64-decode -> ``runner.segment_raster`` ->
FeatureCollection dict) that ``build_geosam_pyfunc``'s ``PythonModel.predict`` merely
wraps. Likewise the I/O contract is plain data (``PREDICT_INPUT_COLUMNS`` /
``PREDICT_OUTPUT_COLUMNS``); ``geosam_signature()`` converts it to a real mlflow
``ModelSignature`` lazily. All network calls (mlflow logging/registration, serving
endpoint create, serving endpoint query) are behind ``_mlflow_log_and_register`` /
``_serving_create`` / ``_serving_query`` seams that tests monkeypatch.

Light-only, Serverless-safe: no ``spark.conf``/``_jvm``/``.rdd``/``sparkContext`` here.
"""

import base64
import json
import os
import time

import shapely.geometry
import shapely.wkb

# Plain-data I/O contract for the pyfunc -- (column name, mlflow/pandas dtype string).
# geosam_signature() converts this to a real mlflow ModelSignature lazily;
# _predict_geojson enforces the same names without needing mlflow installed at all.
PREDICT_INPUT_COLUMNS = [("image_b64", "string")]
PREDICT_OUTPUT_COLUMNS = [("geojson", "string")]

# Default wheel_path for register_to_unity_gateway: a BARE local/Volume path (never a
# file:// URI -- that form is only meaningful to pip, and even then the Model Serving
# container build cannot reach a Volume at all). Matches the sample-data Volume layout
# used elsewhere for wheel staging; override wheel_path= with the actual staged
# wheel's path/version at call time.
DEFAULT_WHEEL_PATH = (
    "/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.5.2-py3-none-any.whl"
)


def _bundled_pip_reqs(wheel_path) -> list:
    """The default ``pip_requirements`` for ``mlflow.pyfunc.log_model`` when
    ``wheel_path`` is passed via ``code_paths`` (mlflow's private-wheel pattern):
    mlflow copies the wheel into the model at ``code/<basename>`` (no subdirs
    preserved), so pip must install from that BUNDLED path, not the original
    local/Volume path -- referencing the original path (or a ``file:///Volumes/...``
    URI) is exactly the bug this fixes, since the Model Serving container build has no
    access to the workspace's UC Volumes. Pulls in the ``models_gpu_env5`` extra (pins
    the CUDA-12 torch + opencv 4.x) plus segment-geospatial, the GeoSAM backend.
    """
    basename = os.path.basename(wheel_path)
    return [f"code/{basename}[models_gpu_env5]", "segment-geospatial"]


# Default pip_requirements, derived from DEFAULT_WHEEL_PATH -- kept for
# introspection/back-compat; register_to_unity_gateway recomputes this from whatever
# wheel_path it is actually given, so a caller-supplied wheel_path is honored here too.
DEFAULT_PIP_REQS = tuple(_bundled_pip_reqs(DEFAULT_WHEEL_PATH))


def _in_databricks_runtime() -> bool:
    """True when executing inside a Databricks runtime -- notebook, job, Serverless, or
    model-serving. There, MLflow and the deploy client authenticate AMBIENTLY (the
    runtime injects credentials and the ``databricks``/``databricks-uc`` URIs resolve
    without a named profile), so a ``~/.databrickscfg`` profile is neither present nor
    needed. Off-cluster (local dev) this is False and the caller's profile is applied to
    reach the remote workspace. Env-var based so this module stays mlflow-free (R4) and
    unit-testable. ``DATABRICKS_RUNTIME_VERSION`` is set on classic DBR and Serverless
    alike (e.g. ``client.*``); ``DB_IS_DRIVER`` is a secondary signal."""
    import os

    return bool(
        os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("DB_IS_DRIVER")
    )


def _apply_profile_auth(profile) -> None:
    """Point MLflow / the deploy client at ``profile`` ONLY when running off-cluster.

    On-cluster, setting ``DATABRICKS_CONFIG_PROFILE`` to a dev profile name (e.g.
    ``oauth-fe``) makes MLflow look up a ``~/.databrickscfg`` entry that does not exist in
    the job container and fail with 'Reading Databricks credential configuration failed'
    -- the observed nb3 ``register_to_unity_gateway`` crash on Serverless GPU. So we leave
    ambient auth untouched on-cluster and honor the caller's profile only off-cluster.
    ``profile=None`` is always a no-op."""
    import os

    if profile is not None and not _in_databricks_runtime():
        os.environ["DATABRICKS_CONFIG_PROFILE"] = profile


def _image_b64_from_input(model_input) -> str:
    """``model_input`` is either a plain dict (direct calls, unit tests) or a
    single-row pandas DataFrame -- the shape mlflow hands a ``PythonModel.predict`` for
    a ``dataframe_records`` request against a single-column signature."""
    if hasattr(model_input, "iloc"):
        return model_input.iloc[0]["image_b64"]
    return model_input["image_b64"]


def _rows_to_feature_collection(df) -> dict:
    """``runner.segment_raster``'s DataFrame (``label:int``, ``geom:bytes`` WKB,
    ``score:float``) -> a GeoJSON ``FeatureCollection`` dict."""
    features = []
    for row in df.to_dict(orient="records"):
        geom = shapely.wkb.loads(bytes(row["geom"]))
        features.append(
            {
                "type": "Feature",
                "geometry": shapely.geometry.mapping(geom),
                "properties": {
                    "label": int(row["label"]),
                    "score": float(row["score"]),
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def _predict_geojson(
    model_input, *, model_type: str = "vit_h", weights=None, gpus=1
) -> dict:
    """The predict LOGIC, mlflow-free (R4): base64-decode the image, call
    ``runner.segment_raster`` (via the module reference so
    ``monkeypatch.setattr("databricks.labs.gbx.models.runner.segment_raster", ...)`` is
    honored -- a ``from ... import segment_raster`` binding captured at def time would
    NOT see a later monkeypatch on the ``runner`` module attribute), and convert the
    resulting rows into a GeoJSON FeatureCollection. ``build_geosam_pyfunc``'s
    ``PythonModel.predict`` is a thin mlflow wrapper around this.

    The raw (base64-decoded) image bytes are passed straight through to
    ``segment_raster`` -- its ``tile_or_path`` argument already accepts raw GTiff/image
    bytes (see runner.py's ``_open_input``), so no separate rasterio decode is needed
    here.
    """
    from databricks.labs.gbx.models import geosam, runner

    raw = base64.b64decode(_image_b64_from_input(model_input))

    def _segmenter(image):
        handle = geosam.load_geosam(model_type=model_type, weights=weights)
        return geosam.segment(handle, image)

    df = runner.segment_raster(raw, segmenter=_segmenter, gpus=gpus)
    return {"geojson": json.dumps(_rows_to_feature_collection(df))}


def build_geosam_pyfunc(*, model_type: str = "vit_h", weights=None):
    """Build the GeoSAM mlflow pyfunc. ``predict(ctx, model_input)`` decodes
    ``image_b64``, calls ``gbx.models.runner.segment_raster`` (so geobrix rides into
    the served model's env), and returns ``{"geojson": <FeatureCollection str>}``.

    mlflow is imported lazily here, and the ``PythonModel`` subclass is defined inside
    this function rather than at module scope -- R4: this module must import cleanly
    with no mlflow installed.
    """
    import mlflow.pyfunc

    class _GeoSamPyfunc(mlflow.pyfunc.PythonModel):
        def predict(self, context, model_input, params=None):
            return _predict_geojson(model_input, model_type=model_type, weights=weights)

    return _GeoSamPyfunc()


def geosam_signature():
    """mlflow ``ModelSignature``: input ``image_b64:string`` -> output
    ``geojson:string``. Lazily converts ``PREDICT_INPUT_COLUMNS`` /
    ``PREDICT_OUTPUT_COLUMNS`` (the plain-data I/O contract ``_predict_geojson`` honors
    without mlflow) into real mlflow ``Schema``/``ColSpec`` objects."""
    from mlflow.models import ModelSignature
    from mlflow.types.schema import ColSpec, Schema

    inputs = Schema([ColSpec(dtype, name) for name, dtype in PREDICT_INPUT_COLUMNS])
    outputs = Schema([ColSpec(dtype, name) for name, dtype in PREDICT_OUTPUT_COLUMNS])
    return ModelSignature(inputs=inputs, outputs=outputs)


def register_to_unity_gateway(
    pyfunc,
    name,
    *,
    profile,
    signature=None,
    wheel_path=DEFAULT_WHEEL_PATH,
    pip_reqs=None,
) -> str:
    """Register ``pyfunc`` to the Unity Catalog model registry ("Unity Gateway") under
    the three-level UC name ``name`` (``catalog.schema.model``). Returns the resulting
    UC model version as a string.

    Bundles the geobrix wheel WITH the model (mlflow's private-wheel pattern) rather
    than installing it from a Volume path the Model Serving container build cannot
    reach: ``wheel_path`` (a bare local/Volume path, default ``DEFAULT_WHEEL_PATH``) is
    always passed through as ``code_paths=[wheel_path]``, which copies it into the
    model at ``code/<basename>``. ``pip_reqs`` defaults to ``_bundled_pip_reqs(
    wheel_path)`` -- installing geobrix from that BUNDLED path (plus
    segment-geospatial) -- and is computed fresh from ``wheel_path`` so an overridden
    ``wheel_path`` is honored even when ``pip_reqs`` is left at its default. Pass
    ``pip_reqs`` explicitly to override the computed default entirely.

    No mlflow import here -- all of it (including ``signature`` defaulting to
    ``geosam_signature()`` when ``None``) happens inside ``_mlflow_log_and_register``,
    which real callers exercise for real and tests monkeypatch.
    """
    pip_reqs = list(pip_reqs) if pip_reqs is not None else _bundled_pip_reqs(wheel_path)
    return _mlflow_log_and_register(
        pyfunc=pyfunc,
        name=name,
        profile=profile,
        signature=signature,
        pip_reqs=pip_reqs,
        code_paths=[wheel_path],
    )


def _mlflow_log_and_register(
    *, pyfunc, name, profile, signature, pip_reqs, code_paths
) -> str:
    """Real seam: selects ``profile`` (``DATABRICKS_CONFIG_PROFILE`` -- never
    auto-selected by this module; the caller chooses), sets the UC registry URI, logs
    ``pyfunc`` with ``code_paths`` (bundles the geobrix wheel into the model),
    ``pip_reqs`` (installs it from the bundled copy), and ``signature`` (defaulting to
    ``geosam_signature()`` when ``None``), and registers under the three-level UC
    ``name``. Returns the resulting model version as a string.

    ``pip_requirements`` is passed EXPLICITLY (never left for mlflow to infer) -- with
    geobrix imported in-process for the wheel path resolution, inference could
    re-capture the current environment's ``geobrix @ file:///Volumes/...`` requirement,
    reintroducing the exact unreachable-URI bug this module fixes.

    Mocked by tests -- never exercised without mlflow/network installed in CI.
    """
    import mlflow
    import mlflow.pyfunc

    _apply_profile_auth(profile)
    mlflow.set_registry_uri("databricks-uc")
    if signature is None:
        signature = geosam_signature()
    with mlflow.start_run():
        info = mlflow.pyfunc.log_model(
            python_model=pyfunc,
            name="model",
            signature=signature,
            code_paths=code_paths,
            pip_requirements=pip_reqs,
            registered_model_name=name,
        )
    return str(info.registered_model_version)


def create_endpoint(
    name,
    model_version,
    *,
    profile,
    endpoint_name,
    workload_type="GPU_MEDIUM",
    workload_size="Small",
    scale_to_zero=True,
) -> dict:
    """Create a GPU serving endpoint for UC model ``name``@``model_version``. Thin
    wrapper: assembles the served-entity/traffic config; the actual client call +
    readiness poll live in ``_serving_create``, seamed so tests never hit the network.

    ``workload_type`` selects the GPU class (e.g. ``GPU_MEDIUM`` = A10G); ``workload_size``
    (``Small``/``Medium``/``Large``) sets concurrency and is REQUIRED by the Serving API --
    omitting it fails endpoint creation with ``400 workloadSizeId is undefined``.
    """
    served_model_name = f"{name.split('.')[-1]}-{model_version}"
    config = {
        "served_entities": [
            {
                "entity_name": name,
                "entity_version": str(model_version),
                "workload_type": workload_type,
                "workload_size": workload_size,
                "scale_to_zero_enabled": scale_to_zero,
            }
        ],
        "traffic_config": {
            "routes": [
                {"served_model_name": served_model_name, "traffic_percentage": 100}
            ]
        },
    }
    return _serving_create(endpoint_name, config, profile=profile)


def _await_ready(
    get_info, *, timeout_s=1800.0, poll_s=10.0, _sleep=time.sleep, _clock=time.monotonic
) -> dict:
    """Poll ``get_info()`` until the serving endpoint is ready, or raise.

    READY == ``state.ready == "READY"`` AND ``state.config_update == "NOT_UPDATING"``
    (per the databricks-model-serving skill: ``state.ready`` alone can read READY mid
    a version-swap while the old version still serves). Raises ``RuntimeError`` on a
    terminal ``config_update`` state (``UPDATE_FAILED`` / ``UPDATE_CANCELED``), and
    ``TimeoutError`` once ``timeout_s`` elapses without becoming ready. ``_sleep``/
    ``_clock`` are injectable seams so tests can drive this without a real sleep or
    wall-clock wait.
    """
    start = _clock()
    while True:
        info = get_info()
        state = info.get("state", {})
        cu = state.get("config_update")
        if state.get("ready") == "READY" and cu == "NOT_UPDATING":
            return info
        if cu in ("UPDATE_FAILED", "UPDATE_CANCELED"):
            raise RuntimeError(
                f"serving endpoint entered terminal state: config_update={cu}"
            )
        if _clock() - start > timeout_s:
            raise TimeoutError(
                f"serving endpoint not ready after {timeout_s:.0f}s (last config_update={cu})"
            )
        _sleep(poll_s)


def _serving_create(endpoint_name, config, *, profile) -> dict:
    """Real seam: mlflow deployments client ``create_endpoint`` + bounded readiness
    poll via ``_await_ready`` (30 min default timeout -- the GPU/provisioned-throughput
    serving ceiling). Mocked by tests -- never exercised without mlflow/network in CI.
    """
    from mlflow.deployments import get_deploy_client

    _apply_profile_auth(profile)
    client = get_deploy_client("databricks")
    client.create_endpoint(name=endpoint_name, config=config)
    return _await_ready(lambda: client.get_endpoint(endpoint=endpoint_name))


def query(endpoint_name, image, *, profile) -> dict:
    """Base64-encode ``image`` (raw image/GTiff bytes) and query the running GPU
    serving endpoint ``endpoint_name`` via the classical-ML ``dataframe_records`` shape
    the single-string-column ``geosam_signature()`` expects. Real network call happens
    inside ``_serving_query`` -- seamed for tests.
    """
    image_b64 = base64.b64encode(bytes(image)).decode()
    return _serving_query(endpoint_name, image_b64, profile=profile)


def _serving_query(endpoint_name, image_b64, *, profile) -> dict:
    """Real seam: mlflow deployments client ``predict()`` against a running endpoint.
    Mocked by tests -- never exercised without mlflow/network in CI."""
    from mlflow.deployments import get_deploy_client

    _apply_profile_auth(profile)
    client = get_deploy_client("databricks")
    return client.predict(
        endpoint=endpoint_name,
        inputs={"dataframe_records": [{"image_b64": image_b64}]},
    )
